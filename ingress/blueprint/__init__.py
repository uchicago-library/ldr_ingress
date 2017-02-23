import tempfile
from uuid import uuid4
from pathlib import Path
from hashlib import md5 as _md5
import logging


import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from werkzeug.datastructures import FileStorage
from flask import Blueprint, abort
from flask_restful import Resource, Api, reqparse

from pypremis.lib import PremisRecord


BLUEPRINT = Blueprint('ingress', __name__)


BLUEPRINT.config = {}


API = Api(BLUEPRINT)

log = logging.getLogger(__name__)

class Ingress(Resource):
    def post(self):

        def retrieve_obj_id(rec):
            obj = rec.get_object_list()[0]
            obj_id = obj.get_objectIdentifier()[0]
            obj_id_value = obj_id.get_objectIdentifierValue()
            return obj_id_value

        log.info("POST received.")
        log.debug("Parsing arguments")
        parser = reqparse.RequestParser()
        parser.add_argument(
            "md5",
            required=True,
            help="The md5 checksum of the file.",
            type=str
        )
        parser.add_argument(
            "name",
            help="The name of the resource",
            type=str,
            default=None,
            required=False
        )
        parser.add_argument(
            "file",
            required=True,
            help="The file to put into the Long Term Storage environment.",
            type=FileStorage,
            location="files"
        )
        parser.add_argument(
            "accession_id",
            required=True,
            help="The accession to which this file belongs",
            type=str
        )
        args = parser.parse_args()
        log.debug("Arguments parsed")

        # Set up a little working environment, a tmpdir to write files into
        log.debug("Creating a temporary directory to work in.")
        _tmpdir = tempfile.TemporaryDirectory()
        tmpdir = _tmpdir.name

        # Make a placeholder path - note we never use the client provided
        # filename to instantiate the file _ever_ in order to avoid security
        # considerations that would entail.
        in_file_path = str(Path(tmpdir, uuid4().hex))

        # Save the file to a tmp location
        log.debug("Saving file into tmpdir")
        args['file'].save(in_file_path)

        # Generate a baseline md5 of what we now have saved...
        log.info("Generating md5 of received file")
        md5 = None
        with open(in_file_path, 'rb') as f:
            hasher = _md5()
            data = f.read(65536)
            while data:
                hasher.update(data)
                data = f.read(65536)
            md5 = hasher.hexdigest()

        # Be sure it matches what the client provided off the bat
        # TODO: handle failure differently than raising an exception in the
        # future.
        log.info("md5 calculated for file: {}".format(md5))
        if md5 == args['md5']:
            log.debug("md5 matches provided md5")
        else:
            log.critical(
                "md5 mismatch. " +
                "Calculated: {} | Provided: {}".format(
                    md5, args['md5']

                )
            )
            abort(500)

        # Kick the file off the PREMISer, as defined in the config
        log.debug("Transmitting file to PREMISer")
        with open(in_file_path, 'rb') as f:
            data = {"md5": md5}
            if args.get("name"):
                data['originalName'] = args['name']
                data['file'] = ('file', f)
            premis_response_multipart_encoder = MultipartEncoder(data)
            premis_response = requests.post(
                BLUEPRINT.config['PREMIS_ENDPOINT'],
                data=premis_response_multipart_encoder,
                headers={"Content-Type": premis_response_multipart_encoder.content_type},
                stream=True
            )
            try:
                premis_response.raise_for_status()
            except:
                log.critical("Error in transmission to or response from " +
                                 "PREMISer")
            try:
                premis_str = premis_response.content.decode("utf-8")
            except:
                log.critical("Response from PREMISer could not be " +
                                 "decoded as utf-8")

        # Instantiate the PREMIS file we got back, again as a random filename in
        # our working dir
        log.debug("Instantiating PREMIS file")
        premis_path = str(Path(tmpdir, uuid4().hex))
        with open(premis_path, 'w') as f:
            f.write(premis_str)

        log.debug("Reading PREMIS file...")
        # Grab the ID the PREMISer minted
        rec = PremisRecord(frompath=premis_path)
        objID = retrieve_obj_id(rec)
        log.debug("Retrieved PREMIS ID: {}".format(objID))

        # POST the file and the PREMIS up into the materialsuite endpoint
        log.debug("POSTing file to materialsuite endpoint")
        ingest_output = None
        with open(in_file_path, 'rb') as content_stream:
            with open(premis_path, 'rb') as premis_stream:
                materialsuite_multipart_encoder = MultipartEncoder(
                    {"content": ('content', content_stream),
                     "premis": ('premis', premis_stream)}
                )
                ms_response = requests.post(
                    BLUEPRINT.config['MATERIALSUITE_ENDPOINT'],
                    data=materialsuite_multipart_encoder,
                    headers={'Content-Type': materialsuite_multipart_encoder.content_type},
                    stream=True
                )
                try:
                    ms_response.raise_for_status()
                except:
                    log.critical("Error in response from materialsuite " +
                                     "endpoint")
                    abort(500)
                try:
                    ingest_output = ms_response.json()
                except:
                    log.critical("Response from materialsuite endpoint " +
                                     "could not be interpreted as JSON")
                    abort(500)

        # Check to see if the accession identifier exists
        log.debug("Checking the acc exists in the id nest")
        acc_output = {}
        target_acc_url = BLUEPRINT.config['ACCS_ENDPOINT']+args['accession_id'] + "/"
        acc_exists = requests.head(target_acc_url).status_code == 200
        if not acc_exists:
            log.critical("Acc specified ({}) doesn't exist".format(
                args['accession_id'])
            )
            abort(500)
        else:
            log.debug("Acc identifier ({}) detected in id nest".format(
                args['accession_id'])
            )

        # Add the id to the acc record

        log.debug("Adding member to acc")
        acc_response = requests.post(
            BLUEPRINT.config['ACCS_ENDPOINT']+args['accession_id'] + "/",
            data={"member": objID}
        )
        try:
            acc_response.raise_for_status()
        except:
            log.critical("Problem with the response from the idnest")
            abort(500)
        try:
            acc_output["member_addition"] = acc_response.json()
        except:
            log.critical("response from the idnest could not be " +
                             "interpreted as JSON")
            abort(500)

        log.debug("Cleaning up tmpdir")
        # Cleanup
        del _tmpdir

        return {"status": "success",
                "ingest_output": ingest_output,
                "acc_output": acc_output}


@BLUEPRINT.record
def handle_configs(setup_state):
    app = setup_state.app
    BLUEPRINT.config.update(app.config)
    if BLUEPRINT.config.get("TEMPDIR"):
        tempfile.tempdir = BLUEPRINT.config['TEMPDIR']
    if BLUEPRINT.config.get("VERBOSITY"):
        logging.basicConfig(level=BLUEPRINT.config['VERBOSITY'])
    else:
        logging.basicConfig(level="WARN")

API.add_resource(Ingress, "/")

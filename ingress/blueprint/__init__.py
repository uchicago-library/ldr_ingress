import tempfile
import requests
from uuid import uuid4
from pathlib import Path
from hashlib import md5 as _md5


from werkzeug.datastructures import FileStorage
from flask import Blueprint
from flask_restful import Resource, Api, reqparse

from pypremis.lib import PremisRecord


BLUEPRINT = Blueprint('ingress', __name__)


BLUEPRINT.config = {}


API = Api(BLUEPRINT)


class Ingress(Resource):
    def post(self):

        def retrieve_obj_id(rec):
            obj = rec.get_object_list()[0]
            obj_id = obj.get_objectIdentifier()[0]
            obj_id_value = obj_id.get_objectIdentifierValue()
            return obj_id_value

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

        # Set up a little working environment, a tmpdir to write files into
        _tmpdir = tempfile.TemporaryDirectory()
        tmpdir = _tmpdir.name

        # Make a placeholder path - note we never use the client provided
        # filename to instantiate the file _ever_ in order to avoid security
        # considerations that would entail.
        in_file_path = str(Path(tmpdir, uuid4().hex))

        # Save the file to a tmp location
        args['file'].save(in_file_path)

        # Generate a baseline md5 of what we now have saved...
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
        assert(md5 == args['md5'])

        # Kick the file off the PREMISer, as defined in the config
        with open(in_file_path, 'rb') as f:
            data = {"md5": md5}
            if args.get("name"):
                data['originalName'] = args['name']
            premis_response = requests.post(
                BLUEPRINT.config['PREMIS_ENDPOINT'],
                files={"file": f},
                data=data
            )
            premis_response.raise_for_status()
            premis_str = premis_response.content.decode("utf-8")

        # Instantiate the PREMIS file we got back, again as a random filename in
        # our working dir
        premis_path = str(Path(tmpdir, uuid4().hex))
        with open(premis_path, 'w') as f:
            f.write(premis_str)

        # Grab the ID the PREMISer minted
        rec = PremisRecord(frompath=premis_path)
        objID = retrieve_obj_id(rec)

        # POST the file and the PREMIS up into the materialsuite endpoint
        ingest_output = None
        with open(in_file_path, 'rb') as content_stream:
            with open(premis_path, 'rb') as premis_stream:
                ms_response = requests.post(
                    BLUEPRINT.config['MATERIALSUITE_ENDPOINT'],
                    files={"content": content_stream,
                           "premis": premis_stream}
                )
                ms_response.raise_for_status()
                ingest_output = ms_response.json()

        # Check to see if the accession identifier exists
        # If the acc id is specified as "new" mint one
        acc_output = {}
        if args['accession_id'] != "new":
            target_acc_url = BLUEPRINT.config['ACCS_ENDPOINT']+args['accession_id'] + "/"
            acc_exists = requests.head(target_acc_url).status_code == 200
            if not acc_exists:
                raise ValueError("Acc doesn't exist!")
        else:
            acc_create_response = requests.post(
                BLUEPRINT.config['ACCS_ENDPOINT']
            )
            acc_create_response.raise_for_status()
            acc_create_json = acc_create_response.json()
            acc_output['acc_mint'] = acc_create_json
            args['accession_id'] = acc_create_json['Minted'][0]['identifier']

        # Add the id to the acc record
        acc_response = requests.post(
            BLUEPRINT.config['ACCS_ENDPOINT']+args['accession_id'] + "/",
            data={"member": objID}
        )
        acc_response.raise_for_status()
        acc_output["member_addition"] = acc_response.json()

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

API.add_resource(Ingress, "/")

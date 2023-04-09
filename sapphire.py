from sqlalchemy import create_engine
from cerberus import Validator
import time

SCALAR_METHODS = ["volumetric"]


class Sapphire:
    def __init__(self, schema=None, batch=None, **kwargs):
        self.schema = schema
        self.batch = batch
        self.kwargs = kwargs

    def get_batch(self):
        options = {"credentials": {}, "file": {}}

        options.update(self.kwargs)

        if self.batch["type"] == "sql":
            b = self.get_sql_batch(self.batch["query"], options["credentials"])
        else:
            raise Exception("batch type not supported")

        return b

    def generate_schema(self):
        if self.schema["name"] == "volumetric":
            return self.generate_volumetric_schema(self.schema["options"])
        else:
            raise Exception("schema not supported")

    def validate(self):
        v = Validator()

        batch = self.get_batch()
        schema = self.generate_schema()

        if self.schema["name"] in SCALAR_METHODS:
            value = batch["data"][0]
        else:
            value = batch["data"]

        document = {"value": value}

        r = v.validate(document, schema)

        # ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time())) + " UTC"

        validation = {
            "success": r,
            "errors": v.errors,
            "batch": batch,
            "schema": self.schema["name"],
            "timestamp": time.time(),
        }
        return validation

    def load_parameters(self, schema=None, batch=None, kwargs=None):
        if schema:
            self.schema = schema
        if batch:
            self.batch = batch
        if kwargs:
            self.kwargs = kwargs

    @staticmethod
    def get_sql_batch(query, credentials):
        engine = create_engine(
            f"{credentials['sql_protocol']}://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )

        conn = engine.connect()
        r = conn.execute(query).fetchall()
        batch_id = int(time.time() * 100)
        l = list(map(list.pop, list(map(list, r))))

        return {"id": batch_id, "data": l}

    @staticmethod
    def generate_volumetric_schema(options):
        if "min" in options.keys() and "max" in options.keys():
            min = options["min"]
            max = options["max"]
        elif "neighborhood" in options.keys() and "mid_value" in options.keys():
            min = options["mid_value"] - options["neighborhood"]
            max = options["mid_value"] + options["neighborhood"]
        elif "percent" in options.keys() and "mid_value" in options.keys():
            min = int(options["mid_value"] * (1 - options["percent"]))
            max = int(options["mid_value"] * (options["percent"] + 1))
        else:
            raise Exception("Invalid options")

        return {"value": {"min": min, "max": max}}

    def generate_validation_description(self, batch):
        # function to improve the validation description
        # customizing the text in each batch.
        if self.schema["name"] == "volumetric":
            msg = ""

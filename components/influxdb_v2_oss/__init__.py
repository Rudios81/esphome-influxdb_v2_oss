from esphome import automation
from esphome.core import Lambda
import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.const import (
    CONF_ACCURACY_DECIMALS,
    CONF_BINARY_SENSORS,
    CONF_FORMAT,
    CONF_ID,
    CONF_NAME,
    CONF_SENSORS,
    CONF_SENSOR_ID,
    CONF_TEXT_SENSORS,
    CONF_TIME_ID,
    CONF_URL,
)
from esphome.components.http_request import (
    HttpRequestComponent,
)
from esphome.components.time import RealTimeClock
from esphome.components import binary_sensor, sensor, text_sensor

CODEOWNERS = ["@kpfleming"]

CONF_BACKLOG_DRAIN_BATCH = "backlog_drain_batch"
CONF_BACKLOG_MAX_DEPTH = "backlog_max_depth"
CONF_BUCKET = "bucket"
CONF_HTTP_REQUEST_ID = "http_request_id"
CONF_MEASUREMENTS = "measurements"
CONF_MEASUREMENT_ID = "measurement_id"
CONF_ORGANIZATION = "organization"
CONF_RAW_STATE = "raw_state"
CONF_TAGS = "tags"
CONF_TOKEN = "token"

SENSOR_FORMATS = {
    "float": "f",
    "integer": "i",
    "unsigned_integer": "u",
}

influxdb_ns = cg.esphome_ns.namespace("influxdb")
InfluxDB = influxdb_ns.class_("InfluxDB", cg.Component)
InfluxDBStatics = influxdb_ns.namespace("InfluxDB")
Measurement = influxdb_ns.class_("Measurement")
BinarySensorField = influxdb_ns.class_("BinarySensorField")
SensorField = influxdb_ns.class_("SensorField")
TextSensorField = influxdb_ns.class_("TextSensorField")


def valid_identifier(value):
    value = cv.string_strict(value)

    if value[0] == "_":
        raise cv.Invalid(f"Identifiers cannot begin with '_': {value}")

    return value


def escape_identifier(value):
    return "".join(["\\" + c if c in " ,=\\" else c for c in value])


def validate_sensor_config(config):
    if (CONF_ACCURACY_DECIMALS in config) and (config[CONF_FORMAT] != "float"):
        raise cv.Invalid(
            f"{CONF_ACCURACY_DECIMALS} cannot be used with the '{config[CONF_FORMAT]}' format"
        )

    return config


MEASUREMENTS_SCHEMA = cv.All(
    cv.Schema(
        {
            cv.Required(CONF_ID): cv.declare_id(Measurement),
            cv.Required(CONF_BUCKET): cv.string,
            cv.Required(CONF_NAME): valid_identifier,
            cv.Optional(CONF_TAGS): cv.Schema({valid_identifier: cv.string}),
            cv.Optional(CONF_BINARY_SENSORS): cv.ensure_list(
                cv.maybe_simple_value(
                    cv.Schema(
                        {
                            cv.GenerateID(): cv.declare_id(BinarySensorField),
                            cv.Required(CONF_SENSOR_ID): cv.use_id(binary_sensor.BinarySensor),
                            cv.Optional(CONF_NAME): valid_identifier,
                        }
                    ),
                    key=CONF_SENSOR_ID,
                )
            ),
            cv.Optional(CONF_SENSORS): cv.ensure_list(
                cv.maybe_simple_value(
                    cv.Schema(
                        {
                            cv.GenerateID(): cv.declare_id(SensorField),
                            cv.Required(CONF_SENSOR_ID): cv.use_id(sensor.Sensor),
                            cv.Optional(CONF_NAME): valid_identifier,
                            cv.Optional(CONF_FORMAT, default="float"): cv.enum(SENSOR_FORMATS),
                            cv.Optional(CONF_ACCURACY_DECIMALS,): cv.positive_not_null_int,
                            cv.Optional(CONF_RAW_STATE, default=False): cv.boolean,
                        }
                    ),
                    validate_sensor_config,
                    key=CONF_SENSOR_ID,
                )
            ),
            cv.Optional(CONF_TEXT_SENSORS): cv.ensure_list(
                cv.maybe_simple_value(
                    cv.Schema(
                        {
                            cv.GenerateID(): cv.declare_id(TextSensorField),
                            cv.Required(CONF_SENSOR_ID): cv.use_id(text_sensor.TextSensor),
                            cv.Optional(CONF_NAME): valid_identifier,
                            cv.Optional(CONF_RAW_STATE, default=False): cv.boolean,
                        }
                    ),
                    key=CONF_SENSOR_ID,
                )
            ),
        }
    ),
    cv.has_at_least_one_key(CONF_BINARY_SENSORS, CONF_SENSORS, CONF_TEXT_SENSORS),
)


def validate_config(config):
    if (CONF_BACKLOG_MAX_DEPTH in config) and (CONF_TIME_ID not in config):
        raise cv.Invalid(f"{CONF_BACKLOG_MAX_DEPTH} requires a 'time' component.")

    if (CONF_BACKLOG_DRAIN_BATCH in config) and (CONF_BACKLOG_MAX_DEPTH not in config):
        raise cv.Invalid(
            f"{CONF_BACKLOG_DRAIN_BATCH} requires {CONF_BACKLOG_MAX_DEPTH} to be set."
        )

    return config


CONFIG_SCHEMA = cv.All(
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(InfluxDB),
            cv.GenerateID(CONF_HTTP_REQUEST_ID): cv.use_id(HttpRequestComponent),
            cv.Required(CONF_URL): cv.url,
            cv.Required(CONF_ORGANIZATION): cv.string,
            cv.Optional(CONF_TOKEN): cv.string,
            cv.OnlyWith(CONF_TIME_ID, "time"): cv.use_id(RealTimeClock),
            cv.Optional(CONF_TAGS): cv.Schema({valid_identifier: cv.string}),
            cv.Optional(CONF_BACKLOG_MAX_DEPTH): cv.int_range(min=1, max=200),
            cv.Optional(CONF_BACKLOG_DRAIN_BATCH): cv.int_range(min=1, max=20),
            cv.Required(CONF_MEASUREMENTS): cv.ensure_list(MEASUREMENTS_SCHEMA),
        }
    ).extend(cv.COMPONENT_SCHEMA),
    validate_config,
)


async def to_code(config):
    db = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(db, config)

    http = await cg.get_variable(config[CONF_HTTP_REQUEST_ID])
    cg.add(db.set_http_request(http))

    url = config[CONF_URL]
    if url[-1] == "/":
        url = url[0:-1]

    org = config[CONF_ORGANIZATION]

    cg.add(db.set_url(f"{url}/api/v2/write?org={org}&precision=s"))

    if token := config.get(CONF_TOKEN):
        cg.add(db.set_token(token))

    if clock_id := config.get(CONF_TIME_ID):
        clock = await cg.get_variable(clock_id)
        cg.add(db.set_clock(clock))

        if backlog_max_depth := config.get(CONF_BACKLOG_MAX_DEPTH):
            cg.add(db.set_backlog_max_depth(backlog_max_depth))

            if backlog_drain_batch := config.get(CONF_BACKLOG_DRAIN_BATCH):
                cg.add(db.set_backlog_drain_batch(backlog_drain_batch))

    parent_tag_string = ""
    if tags := config.get(CONF_TAGS):
        parent_tag_string = "".join(
            [f",{escape_identifier(k)}={escape_identifier(v)}" for k, v in tags.items()]
        )

    for measurement in config.get(CONF_MEASUREMENTS):
        meas = cg.new_Pvariable(measurement[CONF_ID], db)

        bucket = measurement[CONF_BUCKET]
        cg.add(meas.set_bucket(bucket))

        tag_string = ""
        if tags := measurement.get(CONF_TAGS):
            tag_string = "".join(
                [
                    f",{escape_identifier(k)}={escape_identifier(v)}"
                    for k, v in tags.items()
                ]
            )

        cg.add(
            meas.set_line_prefix(
                f"{escape_identifier(measurement[CONF_NAME])}{parent_tag_string}{tag_string}"
            )
        )

        if binary_sensors := measurement.get(CONF_BINARY_SENSORS):
            for conf in binary_sensors:
                var = cg.new_Pvariable(conf[CONF_ID])
                sens = await cg.get_variable(conf[CONF_SENSOR_ID])
                cg.add(var.set_sensor(sens))

                if name := conf.get(CONF_NAME):
                    cg.add(var.set_field_name(escape_identifier(name)))

                cg.add(meas.add_binary_sensor_field(var))

        if sensors := measurement.get(CONF_SENSORS):
            for conf in sensors:
                var = cg.new_Pvariable(conf[CONF_ID])
                sens = await cg.get_variable(conf[CONF_SENSOR_ID])
                cg.add(var.set_sensor(sens))

                cg.add(var.set_format(conf[CONF_FORMAT]))
                cg.add(var.set_raw_state(conf[CONF_RAW_STATE]))

                if accuracy := conf.get(CONF_ACCURACY_DECIMALS):
                    cg.add(var.set_accuracy_decimals(accuracy))

                if name := conf.get(CONF_NAME):
                    cg.add(var.set_field_name(escape_identifier(name)))

                cg.add(meas.add_sensor_field(var))

        if text_sensors := measurement.get(CONF_TEXT_SENSORS):
            for conf in text_sensors:
                var = cg.new_Pvariable(conf[CONF_ID])
                sens = await cg.get_variable(conf[CONF_SENSOR_ID])
                cg.add(var.set_sensor(sens))

                cg.add(var.set_raw_state(conf[CONF_RAW_STATE]))

                if name := conf.get(CONF_NAME):
                    cg.add(var.set_field_name(escape_identifier(name)))

                cg.add(meas.add_text_sensor_field(var))


CONF_INFLUXDB_PUBLISH = "influxdb.publish"
INFLUXDB_PUBLISH_ACTION_SCHEMA = automation.maybe_simple_id(
    {
        cv.GenerateID(): cv.use_id(Measurement),
    }
)


@automation.register_action(
    CONF_INFLUXDB_PUBLISH, automation.LambdaAction, INFLUXDB_PUBLISH_ACTION_SCHEMA
)
async def influxdb_publish_action_to_code(config, action_id, template_arg, args):
    meas = await cg.get_variable(config[CONF_ID])
    text = str(cg.statement(InfluxDBStatics.publish_action(meas)))
    lambda_ = await cg.process_lambda(Lambda(text), args, return_type=cg.void)
    return cg.new_Pvariable(action_id, template_arg, lambda_)


CONF_INFLUXDB_PUBLISH_BATCH = "influxdb.publish_batch"
INFLUXDB_PUBLISH_BATCH_ACTION_SCHEMA = cv.ensure_list(cv.use_id(Measurement))


@automation.register_action(
    CONF_INFLUXDB_PUBLISH_BATCH,
    automation.LambdaAction,
    INFLUXDB_PUBLISH_BATCH_ACTION_SCHEMA,
)
async def influxdb_publish_batch_action_to_code(config, action_id, template_arg, args):
    meas = [await cg.get_variable(m) for m in config]
    text = str(cg.statement(InfluxDBStatics.publish_batch_action(meas)))
    lambda_ = await cg.process_lambda(Lambda(text), args, return_type=cg.void)
    return cg.new_Pvariable(action_id, template_arg, lambda_)

import sys

# import threading
import base64
import hmac
import hashlib
import asyncio
from json import dumps
from typing import List

# import json
sys.path.insert(0, "..")

from opcua import Client
import time

from threading import Thread

from azure.iot.device.aio import ProvisioningDeviceClient
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from azure.iot.device import MethodResponse
from azure.iot.device import exceptions

# device settings - FILL IN YOUR VALUES HERE
scope_id = ""
group_symmetric_key = ""


# optional device settings - CHANGE IF DESIRED/NECESSARY
provisioning_host = "global.azure-devices-provisioning.net"
device_id = "factory_client"
model_id = ""  # This model is available in the root of the Github repo (Failover.json) and can be imported into your Azure IoT central application

variable_nodes = []
send_frequency = 1
incoming_queue = []

await_timeout = 4.0
use_websockets = True
device_client = None
max_connection_attempt = 3


class SubsriptionHandler(object):
    def datachange_notification(self, node, val, data):
        # don't try and do anything with the node as network calls to the server are not allowed outside of the main thread - so we just queue it
        incoming_queue.append(
            {
                "source_time_stamp": data.monitored_item.Value.SourceTimestamp.strftime(
                    "%m/%d/%Y, %H:%M:%S"
                ),
                "nodeid": node,
                "value": val,
            }
        )

    def event_notification(self, event):
        print("Python: New event", event)


def walk_objects(object, dump_data=None):
    variables = object.get_variables()
    object_name = object.get_display_name().to_string()
    if object_name != "Server":
        if object_name != "Objects":
            dump_data[object_name] = {}
            cur_obj = dump_data[object_name]
        else:
            cur_obj = dump_data

        # incoming_queue.append(
        #     {
        #         "source_time_stamp": time.strftime(
        #             "%m/%d/%Y, %H:%M:%S", time.localtime()
        #         ),
        #         "nodeid": object.nodeid.to_string(),
        #     }
        # )
        if len(variables) == 0:
            for child in object.get_children():
                walk_objects(child, cur_obj)
        else:
            walk_variables(object, cur_obj)


# stack is redundant right now but need to move server to nodes with node class as an attribute
def walk_variables(object, dump_data: dict):
    var_stack = []
    dump_data["tags"] = []
    variables = object.get_children()
    for variable in variables:
        var_stack.append(variable)

    while len(var_stack) > 0:
        variable = var_stack.pop()
        children = variable.get_children()
        if len(children) == 0:
            var_id = variable.nodeid.to_string()
            var_name = variable.get_display_name().to_string()
            var_type = variable.get_data_type_as_variant_type().name
            variable_nodes.append(var_id)
            dump_data["tags"].append(
                {"nodeId": var_id, "name": var_name, "type": var_type}
            )
            print(f"    - {var_id} : {var_name}")
            if var_type == "ExtensionObject":
                # get the struct members
                for sub_var in variable.get_value().ua_types:
                    print("        - {}".format(sub_var[0]))
        else:
            var_stack.append(variable)


# def opcua_read_thread(client):
#     while True:
#         for node_id in variable_nodes:
#             node = client.get_node(node_id)
#             name = node.get_display_name().to_string()
#             value = node.get_value()
#             print("{} - {}".format(name, value))
#         time.sleep(send_frequency)


def json_dump_struct(struct_value):
    value = "{"
    first = True
    for sub_var in struct_value.ua_types:
        if not first:
            value = value + ", "
        else:
            first = False
        value = value + f'"{sub_var[0]}":'
        if (
            type(getattr(struct_value, sub_var[0])) == int
            or type(getattr(struct_value, sub_var[0])) == float
            or type(getattr(struct_value, sub_var[0])) == bool
        ):
            value = value + str(getattr(struct_value, sub_var[0]))
        elif str(type(getattr(struct_value, sub_var[0]))) == "string":
            value = value + f'"{getattr(struct_value, sub_var[0])}"'
        elif str(type(getattr(struct_value, sub_var[0]))).startswith("<class"):
            value = value + json_dump_struct(getattr(struct_value, sub_var[0]))
    return value + "}"


async def send_to_central(data):
    if device_client and device_client.connected:
        if "value" not in data:
            value = '"N/A"'
        elif (
            type(data["value"]) == int
            or type(data["value"]) == float
            or type(data["value"]) == bool
        ):
            value = data["value"]
        elif str(type(data["value"])) == "string":
            value = f'"{data["value"]}"'
        elif str(type(data["value"])).startswith("<class"):
            value = json_dump_struct(data["value"])
        else:
            value = '"N/A"'

        payload = f'{{"nodeid": "{data["nodeid"]}", "name": "{data["name"]}", "source_time_stamp": "{data["source_time_stamp"]}", "value": {value}}}'
        print("sending message: %s" % (payload))
        msg = Message(payload)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"
        try:
            # await asyncio.wait_for(device_client.send_message(msg), timeout=await_timeout)
            await device_client.send_message(msg)
            print("completed sending message")
        except asyncio.TimeoutError:
            print("call to send message to IoT Central timed out")


async def incoming_queue_processor():
    while True:
        if len(incoming_queue) > 0:
            data = incoming_queue.pop(0)
            node = opcua_client.get_node(data["nodeid"])
            data["name"] = node.get_display_name().Text
            print(
                "[{}] {} - {}".format(
                    data["source_time_stamp"],
                    data["name"],
                    data["value"] if "value" in data else "",
                )
            )
            await send_to_central(data)
            await asyncio.sleep(2)


# derives a symmetric device key for a device id using the group symmetric key
def derive_device_key(device_id, group_symmetric_key):
    message = device_id.encode("utf-8")
    signing_key = base64.b64decode(group_symmetric_key.encode("utf-8"))
    signed_hmac = hmac.HMAC(signing_key, message, hashlib.sha256)
    device_key_encoded = base64.b64encode(signed_hmac.digest())
    return device_key_encoded.decode("utf-8")


# connect is not optimized for caching the IoT Hub hostname so all connects go through Device Provisioning Service (DPS)
# a strategy here would be to try just the hub connection using a cached IoT Hub hostname and if that fails fall back to a full DPS connect
async def connect():
    global device_client

    trying_to_connect = True
    device_symmetric_key = derive_device_key(device_id, group_symmetric_key)

    connection_attempt_count = 0
    connected = False
    while not connected and connection_attempt_count < max_connection_attempt:
        provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=provisioning_host,
            registration_id=device_id,
            id_scope=scope_id,
            symmetric_key=device_symmetric_key,
            websockets=use_websockets,
        )

        if model_id != "":
            provisioning_device_client.provisioning_payload = '{"iotcModelId":"%s"}' % (
                model_id
            )
        registration_result = None

        try:
            registration_result = await provisioning_device_client.register()
        except (
            exceptions.CredentialError,
            exceptions.ConnectionFailedError,
            exceptions.ConnectionDroppedError,
            exceptions.ClientError,
            Exception,
        ) as e:
            print("DPS registration exception: " + e)
            connection_attempt_count += 1

        if registration_result.status == "assigned":
            dps_registered = True

        if dps_registered:
            device_client = IoTHubDeviceClient.create_from_symmetric_key(
                symmetric_key=device_symmetric_key,
                hostname=registration_result.registration_state.assigned_hub,
                device_id=registration_result.registration_state.device_id,
                websockets=use_websockets,
            )

        try:
            await device_client.connect()
            trying_to_connect = False
            connected = True
            print("connected to central")

        except Exception as e:
            print(
                "Connection failed, retry %d of %d"
                % (connection_attempt_count, max_connection_attempt)
            )
            connection_attempt_count += 1

    return connected


async def main(dump_file=None):
    try:
        # connect to IoT Central
        if dump_file or await connect():

            # connect to the OPC-UA server
            opcua_client.session_timeout = 600000
            opcua_client.connect()
            opcua_client.load_type_definitions()

            root = opcua_client.get_root_node()

            # walk the objects and variable tree
            objects = root.get_child(["0:Objects"])
            dump_data = [{}]
            walk_objects(objects, dump_data=dump_data[0])
            if dump_file:
                file1 = open(dump_file, "w")
                file1.write(dumps(dump_data))
                file1.close()
                return

            # use subscription to get values
            handler = SubsriptionHandler()
            subscription = opcua_client.create_subscription(500, handler)
            for node in variable_nodes:
                node = opcua_client.get_node(node)
                handle = subscription.subscribe_data_change(node)

            # need to process the incoming data outside the subscription notification so we can get information on the node
            tasks = []
            tasks.append(asyncio.create_task(incoming_queue_processor()))
            await asyncio.gather(*tasks)

            # opcua_polling_thread = Thread(target = opcua_read_thread, args = (client, ))
            # opcua_polling_thread.start()
            # opcua_polling_thread.join()

            # queue_handler_thread.join()

            # finally, disconnect
            print("Disconnecting from IoT Hub")
            device_client.disconnect()

        else:
            print(
                "Cannot connect to Azure IoT Central please check the application settings and machine connectivity"
            )
    except Exception as e:
        print(f"Exception: {str(e)}")
    finally:
        opcua_client.disconnect()


if __name__ == "__main__":
    opcua_client = Client("opc.tcp://localhost:4840/widget_co/server/")
    loop = asyncio.run(
        main(sys.argv[2] if len(sys.argv) > 1 and sys.argv[1] == "--dump" else None)
    )

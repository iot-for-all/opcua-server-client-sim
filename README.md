# opcua-server-client-sim

## Prerequisite

Need Python 3.7 or higher. Install from here https://www.python.org/downloads/

## To run

Install the needed libraries:

```shell
pip install -r .\requirements.txt
```

Run the OPC-UA server with default config from a shell:

```shell
python opcua_server.py
```

Edit the file opcua-client.py to apply the scope id and group SAS key for your IoT Central application:

Add scope-id and group SAS key to lines 22 and 23 of opcua-client.py

```python
scope_id = "<Put your scope id here from IoT Central Administration -> Device connection>"
group_symmetric_key = "<Put your group SAS primary key here from IoT Central Administration -> Device Connection -> SAS-IoT-Devices>"
```

Run the OPC-UA client from a shell:

```shell
python opcua_client.py
```

## To stop

Ctrl-C the python processes in there respective shells. This is no glamour code!

## Dump nodes hierarchy

To export the list of nodes to a JSON file in a compatible format for the IoTC Industrial ADT tool ([https://github.com/iot-for-all/iotc-industrial-adt](https://github.com/iot-for-all/iotc-industrial-adt)) run the OPC-UA client in this way:

```shell
python opcua_client.py --dump <FILE_PATH>
```

def map_schema(line, schema):
    # ToDo: Enforce var type
    line = line.split(schema["DELIMITER"])
    msg = {}
    for key in schema["FIELDS"].keys():
        try:
            msg[key] = line[schema["FIELDS"][key]["index"]]
        except:
            return
        return msg

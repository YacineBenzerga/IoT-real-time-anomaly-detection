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


def add_loc_starttime(line):

    try:
        lon, lat, address, start_timestamp = [
            line[i] for i in ["lat", "lon", "address", "start_timestamp"]]
    except Exception as e:
        print(e)
        return
    return dict(line)

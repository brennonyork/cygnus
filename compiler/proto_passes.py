import globals as globl
import schema_pb2 as pb

# This first pass injects a protobuf object into the dictionary
# with the assigned termType as the TABLE_NAME iff there was a
# TABLE_NAME defined.
def inject_protobuf(d):
    s = pb.Schema()
    if("TABLE_NAME" in d[globl.VAR]):
        s.termType = d[globl.VAR]["TABLE_NAME"]
    else:
        s.termType = ""
    d["PROTOBUF"] = s
    return d

# This will move through each Schema and add the individual term
# into the protobuf.
def enumerate_terms(d):
    locality_lookup ={"R":pb.Term.R,
                      "CF":pb.Term.CF,
                      "CQ":pb.Term.CQ,
                      "CV":pb.Term.CV,
                      "TS":pb.Term.TS,
                      "VAL":pb.Term.VAL}
    def det_term_size(term):
        for const, label in d[globl.PARSE]:
            if(label == term and type(const) is int):
                return int(const)
        return False

    unique_schema = 0
    for dict in d[globl.SCHEMA]:
        for key in dict:
            type_list = globl.str_map(lambda x: x.strip(), dict[key].split('+'))
            offset = 0
            for item in type_list:
                term = d["PROTOBUF"].term.add()
                term.name = item
                term.locality = locality_lookup[key]
                #term.dataformat = ??
                term.location = type_list.index(item)
                term_size = det_term_size(item)
                term.schema = unique_schema
                if(term_size):
                    term.length = term_size
                    if(type(offset) is int):
                        term.offset = offset
                        offset += term_size
                else:
                    offset = "unknown"
        unique_schema += 1
    return d

# This pass is a side effect that will serialize the final protobuf
# object to disk.
def serialize_to_disk(d):
    if("TABLE_NAME" in d[globl.VAR]):
        filename = d[globl.VAR]["TABLE_NAME"]+".pb"
    else:
        filename = "serializedProtobuf.pb"
    with open(filename, "w") as pbf:
        pbf.write(d["PROTOBUF"].SerializeToString())
    return d

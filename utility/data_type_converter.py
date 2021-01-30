import copy


# force all number type to stick to original data type
# input is the dictionary 
def data_type_convert_dict(data):
    fields = copy.copy(data) 
    for i in fields:
        if type(fields[i]) == int:
            fields.update({i:int(fields[i])})
        elif type(fields[i]) == float:
            fields.update({i:float(fields[i])})
        else:
            # keep str type to be the same 
            pass
    return fields
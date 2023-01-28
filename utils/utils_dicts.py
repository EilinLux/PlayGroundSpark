from utils.utils_lists import make_list_if_not
import json 

# Dictionary Operations

def add_to_dict_key(dict_, key, value : str) -> dict:
    # Check if key of dict doesn't map to null
    if dict_[key]:
        # If not, then append the valye if it is not None, and doesn't exist already
        list_ = make_list_if_not(dict_[key])
        if (value not in list_) and (value is not None):
            list_.append(value)
        if len(list_) == 1:
            list_ = list_[0]
    else: 
        list_ = value
    
    dict_[key] = list_
    return dict_

def rmv_from_dict_key(dict_, key, value:str) -> dict:
    list_ = make_list_if_not(dict_[key])
    list_ = list(filter(lambda v: v != value, list_))
    list_ = None if len(list_)==0 else list_[0] if len(list_)==1 else list_
    dict_[key] = list_
    return dict_



def dict_key_exist(dict, key):
    return True if key in dict.keys() else False

def save_dict(dict_, file_path, mode='w'):
    with open(file_path, mode) as f:
        json.dump(dict_, f, indent=4)



# List Operations
def make_list_if_not(obj):
    """ check if obj is a list, if not, it puts it into a list. """
    return obj if is_list(obj) else [obj]

def is_list(obj):
    """ check if obj is list. """
    return True if isinstance(obj, list) else False

def map_list(list_, dict_):
    """
    Maps/renames list values to dictionary values using dictionary keys
    Example:
        map_list(['a', 'b'], {'a':'c'})
        >>>> ['c', 'b']
    """
    return [(dict_[item] if item in dict_.keys() else item) for item in list_]

        
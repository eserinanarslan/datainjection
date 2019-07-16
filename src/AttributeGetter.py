from bson import Code


def get_keys(db, collection):

    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collection].map_reduce(map, reduce, "myresults")
    return result.distinct('_id')
from pig_util import outputSchema

# 
# This is where we write python UDFs (User-Defined Functions) that we can call from pig.
# Pig needs to know the schema of the data coming out of the function, 
# which we specify using the @outputSchema decorator.
#
@outputSchema('example_udf:int')
def example_udf(input_str):
    """
    A simple example function that just returns the length of the string passed in.
    """
    return len(input_str) if input_str else None

@outputSchema('extract_user:chararray')
def extract_user(event_map):
    if 'actor' in event_map:
        return event_map['actor']
    else:
        return None

@outputSchema('extract_repo_identifier:chararray')
def extract_repo_identifier(event_map):
    if 'repository' in event_map:
        if 'organization' in event_map['repository']:
            owner = event_map['repository']['organization']
        else:
            owner = event_map['repository']['owner']
        repo = event_map['repository']['name']
        return "%s/%s" % (owner, repo)
    else:
        return None
    

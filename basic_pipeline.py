import re

import apache_beam as beam
from apache_beam.io import WriteToText

def format_result(word_count):
    """Convert tuples (tokens, count) into a string"""
    (word, count) = word_count
    return "{}: {}".format(word, count)
    
with beam.Pipeline() as p:
    
    lines = p | beam.io.ReadFromText("file.in")
    
    counts = (
        lines 
        | "Split" >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | "PairWithOne" >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    
    output = counts | "Format" >> beam.Map(format_result)
    
    output | WriteToText("file.out")
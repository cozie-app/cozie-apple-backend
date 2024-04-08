"""
Module to sanitize strings that are used for InfluxDB queries.
Purpose:
  - Remove special characters
  - Escape special characters
  - Enforce string types where necessary
Source: https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_tutorial/#special-characters-and-keywords
""" 

# Lists of special characters
remove_character_sql = ['\'', '"', '\\']
escape_character_tag_key = [',', '=', ' ']
escape_character_tag_value = [',', '=', ' ']
escape_character_field_key = [',', '=', ' ']
escape_character_field_value = ['"']
escape_character_measurement = [',', '=', ' ']

def test():
  """
  Test functions that prints 'test'
    
  Arguments
  ----------
    - 

  Returns
  -------
    -
  """
  print('test')
  return

def remove_character(input_string, remove_characters):
  """
  Removes special characters from string
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed
    - remove_characters, list, List of strings/characters, that are removed from input_string

  Returns
  -------
    - output_string, str, result from removing remove_characters from input_string
  """
  output_string = input_string
  for character in remove_characters:
    output_string = output_string.replace(character, '')
  return output_string

def escape_character(input_string, escape_strings):
  """
  Escapes special characters from string
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed
    - escape_characters, list, List of strings/characters, that are escaped in input_string

  Returns
  -------
    - output_string, str, result from escaping escape_characters in input_string
  """
  output_string = input_string
  for character in escape_strings:
    output_string = output_string.replace(character, '\''+character)
  return output_string

def measurement(input_string):
  """
  Removes and escapes sepcial characters from string containing an InfluxDB measurement
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed/escaped

  Returns
  -------
    - output_string, str, result from removing/escaping special characters from input_string
  """
  output_string = str(input_string)
  output_string = remove_character(output_string, remove_character_sql)
  output_string = escape_character(output_string, escape_character_measurement)
  return output_string

def tag_key(input_string):
  """
  Removes and escapes sepcial characters from string containing an InfluxDB tag key
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed/escaped

  Returns
  -------
    - output_string, str, result from removing/escaping special characters from input_string
  """
  output_string = str(input_string)
  output_string = remove_character(output_string, remove_character_sql)
  output_string = escape_character(output_string, escape_character_tag_key)
  return output_string

def tag_value(input_string):
  """
  Removes and escapes sepcial characters from string containing an InfluxDB tag value
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed/escaped

  Returns
  -------
    - output_string, str, result from removing/escaping special characters from input_string
  """
  output_string = remove_character(input_string, remove_character_sql)
  output_string = escape_character(output_string, escape_character_tag_value)
  return output_string

def field_key(input_string):
  """
  Removes and escapes sepcial characters from string containing an InfluxDB field key
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed/escaped

  Returns
  -------
    - output_string, str, result from removing/escaping special characters from input_string
  """
  output_string = str(input_string)
  output_string = remove_character(output_string, remove_character_sql)
  output_string = escape_character(output_string, escape_character_field_key)
  return output_string

def field_value(input_string):
  """
  Removes and escapes sepcial characters from string containing an InfluxDB field value
    
  Arguments
  ----------
    - input_string, str, String from which charcters are removed/escaped

  Returns
  -------
    - output_string, str, result from removing/escaping special characters from input_string
  """
  output_string = str(input_string)
  output_string = remove_character(output_string, remove_character_sql)
  output_string = escape_character(output_string, escape_character_field_value)
  return output_string
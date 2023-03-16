from gigasheet import gigasheet

giga = gigasheet.Gigasheet()

if not giga.api_key:
    raise ValueError('You are missing authentication, please check the README for instructions on setting up authentication.')

print('Great, you are set up with authentication!')

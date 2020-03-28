import os
os.environ['IEX_LIVE_TOKEN'] = 'pk_2c0320a704ed400e9cac91e10d641f66'
os.environ['IEX_SANDBOX_TOKEN'] = 'Tpk_d024741ac58c457eaa3f87c9e426de3b'
os.environ['IEX_ENVIRONMENT'] = 'sandbox'
os.environ['IEX_VERSION'] = 'v1'

if os.environ['IEX_ENVIRONMENT'] == 'live': os.environ['IEX_TOKEN'] = os.environ['IEX_LIVE_TOKEN']
elif os.environ['IEX_ENVIRONMENT'] == 'sandbox': os.environ['IEX_TOKEN'] = os.environ['IEX_SANDBOX_TOKEN']
else: os.environ['IEX_TOKEN'] = os.environ['IEX_LIVE_TOKEN']

if os.environ['IEX_ENVIRONMENT'] == 'sandbox': print ('IEX: YOU ARE IN SANDBOX ENVIRONEMT!')
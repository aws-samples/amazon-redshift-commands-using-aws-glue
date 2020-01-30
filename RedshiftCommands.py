import json
import boto3
import sys
from awsglue.utils import getResolvedOptions
import logging
import pgdb
from urllib.parse import urlparse

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Required Inputs
args = getResolvedOptions(sys.argv, [
        'SQLScript',
        'Secret'
        ])

script = args['SQLScript']
secret = args['Secret']

print('Secret is: %s' % secret)
print('Script is: %s' % script)

# Connect to the cluster
try:
    print ('Getting Connection Info')

    secmgr = boto3.client('secretsmanager')
    secret = secmgr.get_secret_value(SecretId=secret)
    secretString = json.loads(secret["SecretString"])
    user = secretString["user"]
    password = secretString["password"]
    host = secretString["host"]
    port = secretString["port"]
    database = secretString["database"]

    print('Connecting to Redshift: %s' % host)
    conn = pgdb.connect(database=database, host=host, user=user, password=password, port=port)
    print('Successfully Connected to Cluster')

    # create a new cursor for methods to run through
    cursor = conn.cursor()
    statement = ''
    try:
        import boto3
        s3 = boto3.resource('s3')
        o = urlparse(script)
        bucket = o.netloc
        key = o.path
        obj = s3.Object(bucket, key.lstrip('/'))
        statements = obj.get()['Body'].read().decode('utf-8')

        # Optional Input: Parms
        parms = ''
        if ('--{}'.format('Params') in sys.argv):
           params = getResolvedOptions(sys.argv, ['Params'])['Params']
           paramdict = params.split(',')
           for i, param in enumerate(paramdict, start=1):
             statements = statements.replace('${'+str(i)+'}', param.strip())


        for statement in statements.split(';'):
            statement = statement.strip()
            if statement != '':
              print("Running Statement: --%s--" % statement)
              cursor.execute(statement)
              conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(e)
        cursor.close()
        conn.close()
        raise

except Exception as e:
    print(e)
    raise

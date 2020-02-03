# Execute Amazon Redshift Commands using AWS Glue

This project demonstrates how to use a **AWS Glue Python Shell Job** to connect to your **Amazon Redshift** cluster and execute a SQL script stored in Amazon S3.  Amazon Redshift SQL scripts can contain commands such as bulk loading using the COPY statement or data transformation using DDL & DML SQL statements.  Leveraging this strategy, customers can migrate from their existing ETL and ELT infrastructure to a more cost-effective serverless framework.   

## Cloud Formation

The below **AWS Cloud Formation Template** will deploy the necessary components to build your first AWS Glue Job along with necessary components to ensure the connection between the various components is secure.  Once deployed, create AWS Glue triggers and/or workflows to invoke the **RedsdhiftCommands** job passing in any script you'd like.

[![Launch](cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=RedshiftCommands&templateURL=https://s3-us-west-2.amazonaws.com/redshift-immersionday-labs/RedshiftCommands.yaml)

## Solution Components

The following are the re-usable components of the AWS Cloud Formation Template:
1. **AWS Glue Bucket** - This bucket will hold the script which the AWS Glue Python Shell Job will execute.
1. **AWS Glue Connection** - This connection is used to ensure the AWS Glue Job will run within the same Amazon VPC as Amazon Redshift Cluster.
1. **Secrets Manager Secret** - This Secret is stored in the Secrets Manager and will contain the credentials to the Amazon Redshift cluster.
1. **Amazon VPC Endpoints** - 2 Amazon VPC Endpoints are deployed to ensure that Secrets Manager and S3 which are two services which run outside the VPC are accessible within the same Amazon VPC as the AWS Glue Job and Amazon Redshift Cluster.  To setup the enpoints, you must provide network information such as the VPC, Subnet, AZ, Security Group and Route Table.
1. **IAM Role** - This IAM Role is used by the AWS Glue job and requires read access to the Secrets Manager Secret as well as the Amazon S3 location of the python script used in the AWS Glue Job and the Amazon Redshift script.
1. **AWS Glue Job** - This AWS Glue Job will be the compute engine to execute your script. AWS Glue Python Shell jobs are optimal for this type of workload because there is no timeout and it has a very small cost per execution second. The job will take two required parameters and one optional parameter:
* *Secret* - The Secrets Manager Secret ARN containing the Amazon Redshift connection information.
* *SQLScript* - The Amazon S3 Script Loction of the Script in S3 containing the Redshift Script.  Note: The Role created above should have access to read from this location.
* *Params* - (Optional) A comma separated list of script parameters.  To use these parameters in your script use the syntax ${n}.

## Sample Job
Also included in the CloudFormation Template is a sample invocation of the AWS Glue job.  The template will invoke the AWS Glue Job with the following inputs.
* SQLScript: s3://redshift-immersionday-labs/customer.sql
* Secret: ${Secret.Arn}
* Params: ${AWS::StackName}

The job will pull the following SQL script from the Amazon S3 location and replace the parameter ${1} with the StackName and execute the script against your Redshift cluster.

```sql
--customer.sql
drop table if exists customer_${1};
create table customer_${1} (
  C_CUSTKEY bigint NOT NULL PRIMARY KEY,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY bigint,
  C_PHONE varchar(15),
  C_ACCTBAL decimal(18,4),
  C_MKTSEGMENT varchar(10),
  C_COMMENT varchar(117),
  C_UPDATETS timestamp);

insert into customer_${1} values
(1, 'Customer#000000001', '1 Main St.', 1, '555-555-5555', 1234, 'BUILDING', 'comment1', current_timestamp),
(2, 'Customer#000000002', '2 Main St.', 2, '555-555-5555', 1235, 'MACHINERY', 'comment2', current_timestamp),
(3, 'Customer#000000003', '3 Main St.', 3, '555-555-5555', 1236, 'AUTOMOBILE', 'comment3', current_timestamp),
(4, 'Customer#000000004', '4 Main St.', 4, '555-555-5555', 1237, 'HOUSEHOLD', 'comment4', current_timestamp),
(5, 'Customer#000000005', '5 Main St.', 5, '555-555-5555', 1238, 'FURNITURE', 'comment5', current_timestamp);
```

## Code Walkthrough
The following section describes the components of the code which make this solution possible.

### Get the Required Parameters
This code will get value for the inputs **SQLScript** and **Secret**.  It  will error if both are not passed in:
```Python
args = getResolvedOptions(sys.argv, [
        'SQLScript',
        'Secret'
        ])

script = args['SQLScript']
secret = args['Secret']
```

### Get the Cluster Connection Information
This code will first get the connection parameters from the AWS Secrets Manager and use those values to make a connection to Redhshift leveraging the PyGreSQL library.
```Python
secmgr = boto3.client('secretsmanager')
secret = secmgr.get_secret_value(SecretId=secret)
secretString = json.loads(secret["SecretString"])
user = secretString["user"]
password = secretString["password"]
host = secretString["host"]
port = secretString["port"]
database = secretString["database"]
conn = pgdb.connect(database=database, host=host, user=user, password=password, port=port)
```

### Get the contents of the S3 Script
This code will get the S3 object containing the Redshift SQL script and store it into the statements variable.
```Python
import boto3
s3 = boto3.resource('s3')
o = urlparse(script)
bucket = o.netloc
key = o.path
obj = s3.Object(bucket, key.lstrip('/'))
statements = obj.get()['Body'].read().decode('utf-8')
```

### Get the Optional parameters
This code will first determine if the **Params** input was provided, if so, it will get the value and replace the values matching the pattern ${n} in the *statements* variable.
```Python
params = ''
if ('--{}'.format('Params') in sys.argv):
   params = getResolvedOptions(sys.argv, ['Params'])['Params']
   paramdict = params.split(',')
   for i, param in enumerate(paramdict, start=1):
     statements = statements.replace('${'+str(i)+'}', param.strip())                  
```

### Run each Statement
This code will parse and execute each statement using the semicolon (;) as a delimiter.
```Python
for statement in statements.split(';'):
    statement = statement.strip()
    if statement != '':
      print("Running Statement: --%s--" % statement)
      cursor.execute(statement)
      conn.commit()
```

## License

This library is licensed under the MIT-0 License. See the LICENSE file.


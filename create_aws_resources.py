import pandas as pd
import boto3
import json
from tabulate import tabulate
import configparser
import logging
from pprint import pprint
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

LOG_DATA_PATH          = config.get("S3", "LOG_DATA")
LOG_JSON_PATH          = config.get("S3", "LOG_JSONPATH")
SONG_DATA_PATH         = config.get("S3", "SONG_DATA")

#(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

DW_PARAMS = {
    "Param":[
        "DWH_CLUSTER_TYPE", 
        "DWH_NUM_NODES", 
        "DWH_NODE_TYPE", 
        "DWH_CLUSTER_IDENTIFIER",
        "DWH_DB", "DWH_DB_USER", 
        "DWH_DB_PASSWORD",
        "DWH_PORT", 
        "DWH_IAM_ROLE_NAME"
        ],
    "Value":[
        DWH_CLUSTER_TYPE, 
        DWH_NUM_NODES, 
        DWH_NODE_TYPE, 
        DWH_CLUSTER_IDENTIFIER, 
        DWH_DB, DWH_DB_USER,
        DWH_DB_PASSWORD,
        DWH_PORT,
        DWH_IAM_ROLE_NAME
        ]
    }

print(tabulate(pd.DataFrame.from_dict(DW_PARAMS)))


ec2 = boto3.resource("ec2" ,
                   region_name="us-west-2", 
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                  )

s3 = boto3.resource("s3",
                   region_name="us-west-2", 
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                 )

iam = boto3.client("iam",
                   region_name="us-west-2", 
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                  )

redshift = boto3.client("redshift",
                   region_name="us-west-2", 
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                       )

# Replace with correct bucket

bucket =  s3.Bucket("udacity-dend")

# Create IAM Role to connect s3 w/ Redshift
try:
    print('Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description='ALlows Redshift clusters to call AWS services',
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'redshift.amazonaws.com'
                }
            }],
             'Version': '2012-10-17'})
        )


    # Attach appropriate policies
    print('1.2 Attaching Policy')
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME).get('Role').get('Arn')
    print(roleArn)

    with open('dwg.cfg', 'w') as configfile:
        config.write(configfile)
    
except Exception as e:
    print(e)

try:
    response = redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)

def prettyRedshiftProps(props):
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(tabulate(prettyRedshiftProps(myClusterProps)))

DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
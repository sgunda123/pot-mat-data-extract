# Quick Start 
The utility takes one argument, which is a path to the parameters file. The parameters files links to two other files, the attribute mapping which is required, and the source mapping which is optional.

##Building

The main method of the application is at the following path:
**com.reltio.extract.denormalized.service.AttributeExtractReportForPotenMatchesV4**

##Dependencies 

1. gson-2.2.4
2. reltio-cst-core-1.4.6

##Parameters File Example

```
#!paintext
#Common Properties
ENVIRONMENT_URL=sndbx.reltio.com
TENANT_ID=9eBTsa2qL8ZgG7e
AUTH_URL=https://auth.reltio.com/oauth/token
USERNAME=*****
PASSWORD=*****
THREAD_COUNT=20

#Tool specific properties
ENTITY_TYPE=HCP
OV_ATTRIBUTE_FILE=hcp-mapping.properties
OUTPUT_FILE=pm-extracts-hcp.csv
FILE_FORMAT=CSV
HEADER_REQUIRED=Yes
TRANSITIVE_MATCH=false
BATCH_SIZE=100
EXTRACT_PER_RULE=false
FILTER_CONDITION=contains(attributes.LegacyData.ID,'*00')
TARGET_RULE=configuration/entityTypes/Individual/matchGroups/Rule5
SAMPLE_SIZE=1000

```

##Sample Attribute Mapping 

```
#!plaintext

FirstName
LastName
Addresses.AddressLine1
Addresses.AddressLine2
Addresses.City
Addresses.State
Addresses.Zip5
Identifiers.Type
Identifiers.Id
Etc…



```

##Sample Source Mapping

```
#!plaintext

Source1
Source2
Source3
```


##Executing

Command to start the utility.
```
#!plaintext

java -jar pot-mat-extract-{$version}-jar-with-dependencies.jar  job_configuration.properties > $logfilepath$

```

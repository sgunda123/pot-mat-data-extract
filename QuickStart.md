# Quick Start 
The utility takes one argument, which is a path to the parameters file. The parameters files links to two other files, the attribute mapping which is required, and the source mapping which is optional.

##Building

The main method of the application is at the following path:
**com.reltio.extract.denormalized.service.AttributeExtractReportForPotenMatchesV4**

##Dependencies 

1. gson-2.2.4
2. reltio-cst-core-1.3.9
3. reltio-cleanse-5.5.jar

##Parameters File Example

```
#!paintext
AUTH_URL=https://auth.reltio.com/oauth/token
USERNAME=mohan.kumar@reltio.com
PASSWORD=
TENANT_ID=9eBTsa2qL8ZgG7e
RELTIO_SERVER_HOST=sndbx.reltio.com
THREAD_COUNT=20(Recommended)
ENTITY_TYPE=HCP
OV_ATTRIBUTE_FILE_LOCATION=hcp-mapping.properties
OUTPUT_FILE_LOCATION=pm-extracts-hcp.csv
FILE_FORMAT=CSV
HEADER_REQUIRED=Yes
BATCH_SIZE=100


```

##Sample Attribute Mapping 

```
#!plaintext

FirstName
LastName
Address.AddressLine1
Address.AddressLine2
Address.City
Address.State
Address.Zip5
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

java -jar reltio-pm-extractReport-0.0.3.jar  job_configuration.properties > $logfilepath$

```
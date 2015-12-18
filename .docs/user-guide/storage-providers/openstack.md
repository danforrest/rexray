#OpenStack

Making storage management as transparent as the stack

---

## Overview
The OpenStack driver registers a storage driver named `openstack` with the
`REX-Ray` driver manager and is used to connect and manage storage on OpenStack
instances.

## Configuration
The following is an example configuration of the OpenStack driver.

```yaml
openstack:
    authURL:              https://domain.com/openstack
    userID:               0
    userName:             admin
    password:             mypassword
    tenantID:             0
    tenantName:           customer
    domainID:             0
    domainName:           corp
    regionName:           USNW
    availabilityZoneName: Gold
```

For information on the equivalent environment variable and CLI flag names
please see the section on how non top-level configuration properties are
[transformed](./config/#all-other-properties).

## Activating the Driver
To activate the OpenStack driver please follow the instructions for
[activating storage drivers](/user-guide/config#activating-storage-drivers),
using `openstack` as the driver name.

## Examples
Below is a full `rexray.yml` file that works with Openstack.

```yaml
rexray:
  storageDrivers:
  - openstack
openstack:
  authUrl: https://keystoneHost:35357/v2.0/
  username: username
  password: password
  tenantName: tenantName
  regionName: regionName
```

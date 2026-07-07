# Morpheus NetBox Plugin

This plugin provides an IPAM integration between [NetBox](https://netboxlabs.com/netbox/) and [Morpheus](https://morpheusdata.com). It enables network pool sync, IP address sync, IP allocation, host record updates, and IP release workflows from within the Morpheus platform.

## Requirements

| Component | Minimum Version |
|-----------|----------------|
| Morpheus | 7.0.2 |

## Installation

1. Download the latest `.jar` from the [Releases](https://github.com/HewlettPackard/morpheus-netbox-plugin/releases) page, or [build it yourself](#building).
2. In Morpheus, navigate to **Administration → Integrations → Plugins**.
3. Click **Browse** and upload the `.jar` file.
4. The **NetBox** IPAM integration type will appear after the plugin loads.

## Configuration

When adding a NetBox IPAM integration in Morpheus (**Infrastructure → Network → IPAM → Add IPAM Integration**), provide the following:

| Field | Description |
|-------|-------------|
| **API Url** | NetBox API endpoint, e.g. `https://netbox.example.com/`. |
| **Credentials** | Morpheus credential containing either username/password or an API key. |
| **Username** | Local username field used when not selecting a stored credential. |
| **Password** | Local password field used when not selecting a stored credential. |
| **API Token** | Local NetBox API token field used when not selecting a stored credential. |
| **Throttle Rate** | Optional API throttling rate for NetBox requests. |
| **Disable SSL SNI Verification** | Disable SSL SNI verification for the NetBox endpoint. |
| **Inventory Existing** | Import existing IP address records from synced pools. |
| **Deprecate on Delete** | Mark NetBox IP addresses deprecated instead of deleting them when released. |
| **Tags** | Pipe-delimited tags to apply to IP addresses created by Morpheus, e.g. `tag1|tag2`. |

## Features

### IPAM Sync
The plugin registers an `IPAMProvider` for NetBox. The following resources are discovered and kept in sync from NetBox:

- **IP Ranges** — NetBox IP ranges as Morpheus network pools
- **Prefixes** — NetBox prefixes as Morpheus network pools
- **IPv4 Pools** — IPv4 ranges and prefixes
- **IPv6 Pools** — IPv6 ranges and prefixes
- **IP Addresses** — existing NetBox IP address records when **Inventory Existing** is enabled

Any additions, updates, and removals in NetBox are reflected in Morpheus on the next IPAM refresh.

### IP Allocation
Morpheus can allocate addresses into synced NetBox pools. Supported operations include:

- Allocate a requested IPv4 or IPv6 address when one is provided
- Request the next available address from a NetBox range or prefix
- Create or update NetBox IP address records with active status
- Preserve NetBox tenant and VRF details from the parent range or prefix
- Apply configured tags to IP address records created by Morpheus

### Host Record Lifecycle
Morpheus host record operations are mapped to NetBox IP address records. Supported operations include:

- Update NetBox `dns_name` values when hostnames change
- Delete NetBox IP address records when Morpheus releases an address
- Optionally mark released NetBox IP addresses as deprecated instead of deleting them
- Sync NetBox address status back into Morpheus as assigned, reserved, or unmanaged IP records

## Building

```bash
./gradlew shadowJar
```

The plugin JAR will be written to `build/libs/`.

## License

Copyright 2022 the original author or authors. Licensed under the [Apache License, Version 2.0](LICENSE).

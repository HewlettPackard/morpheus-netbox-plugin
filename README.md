# Morpheus NetBox Plugin

The Morpheus NetBox Plugin integrates Morpheus with NetBox to provide IP address management (IPAM) using NetBox IP ranges and prefixes. The plugin communicates with the NetBox REST API to allocate and release IP addresses and synchronise network pools.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Repository structure](#repository-structure)
- [Building the plugin](#building-the-plugin)
- [License](#license)
- [Installing](#installing)
- [Detailed Usage Steps](#detailed-usage-steps)
- [API Endpoints](#api-endpoints)

---

## Features

### IP Address Management

Allocate and release IP addresses from NetBox IP ranges and prefixes within Morpheus. Supports automatic next-available IP selection from prefixes, manual IP entry, existing inventory import, and optional deprecation of IPs on deletion rather than hard delete.

### Cloud Sync

Morpheus synchronises the following NetBox resources for inventory:

- IP ranges (`/api/ipam/ip-ranges/`)
- Prefixes (`/api/ipam/prefixes/`)
- IP address allocations

---

## Requirements

| Requirement | Version |
|-------------|---------|
| Morpheus | 7.0.2 or later |
| Java | 11 or later |
| Gradle | Use the included Gradle wrapper (`./gradlew`) |

Additional prerequisites:

- A running NetBox instance accessible over HTTP or HTTPS from the Morpheus appliance
- A NetBox user account and API token with read/write access to IPAM objects
- Network access from the Morpheus appliance to the NetBox host on the configured port

---

## Repository structure

```
src/main/groovy/com/morpheusdata/netbox/
├── NetBoxPlugin.groovy    - Plugin entry point; registers NetBoxProvider
└── NetBoxProvider.groovy  - IPAMProvider implementation; IPAM operations, sync, OptionTypes
build.gradle, gradle.properties - Build configuration and plugin metadata
```

---

## Building the plugin

Run the following command to compile and package the plugin jar:

```bash
./gradlew clean build
```

The packaged jar will be written to `build/libs/`.

To execute tests, use the following command:

```bash
./gradlew test
```

---

## License

This project is licensed under the Apache License 2.0.

See the [LICENSE](LICENSE) file for details.

---

## Installing

1. Build the plugin (see [Building the plugin](#building-the-plugin)) or download a released jar.
2. In Morpheus, navigate to **Administration > Integrations > Plugins**.
3. Click **Add** and upload the `morpheus-netbox-plugin-<version>.jar` from `build/libs/`.
4. Navigate to **Infrastructure > Networks > IP Pools > Add** and select **NetBox** to configure the integration.

---

## Detailed Usage Steps

### Adding a NetBox IPAM Integration

1. Go to **Infrastructure > Networks > IP Pools > Add**.
2. Select **NetBox** as the pool server type.
3. Enter the **API Url** (e.g. `https://netbox.example.com/`), **Username**, **Password** (or a stored credential), and optionally an **API Token** for token-based authentication.
4. Optionally configure **Throttle Rate**, **Disable SSL SNI Verification**, **Inventory Existing**, **Deprecate on Delete**, and **Tags**.
5. Save. Morpheus connects to NetBox and syncs IP ranges and prefixes as network pools.

### Allocating an IP Address

When provisioning an instance on a network backed by a NetBox prefix, Morpheus calls the NetBox API to allocate the next available IP from that prefix. The IP is registered in NetBox with the instance details.

### Releasing an IP Address

When an instance is decommissioned, Morpheus either deletes the IP allocation from NetBox or sets it to deprecated, depending on the **Deprecate on Delete** setting.

---

## API Endpoints

This plugin communicates with the **NetBox REST API** at the configured service URL. Authentication uses an API token in the `Authorization: Token` header.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `api/ipam/ip-ranges/` | GET | List IP ranges |
| `api/ipam/prefixes/` | GET | List prefixes |
| `api/ipam/prefixes/{id}/available-ips/` | POST | Allocate next available IP from a prefix |
| `api/ipam/ip-addresses/` | GET | List IP addresses |
| `api/ipam/ip-addresses/` | POST | Create an IP address allocation |
| `api/ipam/ip-addresses/{id}/` | PUT | Update an IP address allocation |
| `api/ipam/ip-addresses/{id}/` | DELETE | Delete an IP address allocation |

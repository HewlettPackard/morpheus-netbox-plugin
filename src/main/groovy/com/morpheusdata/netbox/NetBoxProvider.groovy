package com.morpheusdata.netbox

import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.providers.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Single
import org.apache.commons.net.util.SubnetUtils
import io.reactivex.rxjava3.core.Observable
import org.apache.commons.validator.routines.InetAddressValidator


@Slf4j
class NetBoxProvider implements IPAMProvider {
	MorpheusContext morpheusContext
	Plugin plugin
    static String rangesPath = 'api/ipam/ip-ranges/'
    static String prefixesPath = 'api/ipam/prefixes/'
    static String getIpsPath = 'api/ipam/ip-addresses/'
    static String authPath = 'api/users/tokens/provision/'


	NetBoxProvider(Plugin plugin, MorpheusContext morpheusContext) {
		this.morpheusContext = morpheusContext
		this.plugin = plugin
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 *
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'netbox'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'NetBox'
	}

	/**
	 * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
	 * If an input fails validation or authentication information cannot be verified, Error messages should be returned
	 * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
	 * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
	 *
	 * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
	 * @return A response is returned depending on if the inputs are valid or not.
	 */
	@Override
	ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        log.debug("verifyNetworkPoolServer.....")
		ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
		rtn.errors = [:]

        if (!poolServer.name || poolServer.name == ''){
            rtn.errors['name'] = 'Name is required'
        }
		if (!poolServer.serviceUrl || poolServer.serviceUrl == ''){
			rtn.errors['serviceUrl'] = 'NetBox API URL is required'
		}
        if (poolServer.credentialData.type == 'api-key') {
            if((!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
                rtn.errors['servicePassword'] = 'Password is required'
            }
        } else if (poolServer.credentialData.type == 'username-password') {
            if ((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
                rtn.errors['serviceUsername'] = 'Username is required'
            }
            if ((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
                rtn.errors['servicePassword'] = 'Password is required'
            }
        }

		rtn.data = poolServer
		if (rtn.errors.size() > 0){
			rtn.success = false
			return rtn //
		}
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient netboxClient = new HttpApiClient()
        def networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        netboxClient.networkProxy = networkProxy
        def tokenResults
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			boolean hostOnline = false
			try {
				def apiUrlObj = new URL(apiUrl)
				def apiHost = apiUrlObj.host
				def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
                log.debug("verifyNetworkPoolServer - testHostConnectivity.....")
				hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, networkProxy)
			} catch(e) {
				log.error("Error parsing URL {}", apiUrl, e)
			}
			if (hostOnline) {
				opts.doPaging = false
				opts.maxResults = 1
                log.debug("verifyNetworkPoolServer - test login.....")
                tokenResults = login(netboxClient, rpcConfig)
                if (tokenResults.success) {
                    log.debug("verifyNetworkPoolServer - test listNetworks.....")
				    def networkList = listNetworks(netboxClient, tokenResults.tokenString, poolServer, opts)
                    if(networkList.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = networkList.msg ?: 'Error connecting to NetBox'
                    }
                } else {
                    rtn.msg = 'Unable to login to Netbox host'
                }
            } else {
                rtn.msg = 'Host not reachable'
            }
		} catch(e) {
			log.error("verifyPoolServer error: ${e}", e)
		} finally {
			netboxClient.shutdownClient()
		}
		return rtn
	}

	ServiceResponse<NetworkPoolServer> initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		log.info("initializeNetworkPoolServer {} with id {}", poolServer.name, poolServer.id)
		log.debug("initializeNetworkPoolServer: ${poolServer.dump()}")
		def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

	@Override
	ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		return ServiceResponse.success() // no-op
	}

	@Override
	ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		return ServiceResponse.success() // no-op
	}

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        if (!poolServer.enabled) {
            log.warn("Refresh triggered, but pool server NOT enabled, exit refresh...")
            return
        }

        LoginResult tokenResults = new LoginResult()
        def rpcConfig = getRpcConfig(poolServer)
		log.debug("refresh method PoolServer: {}", poolServer.dump())
		HttpApiClient netboxClient = new HttpApiClient()
		netboxClient.throttleRate = poolServer.serviceThrottleRate
        def networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        netboxClient.networkProxy = networkProxy
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			def apiUrlObj = new URL(apiUrl)
			def apiHost = apiUrlObj.host
			def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, networkProxy)

			log.debug("online: {} - {}", apiHost, hostOnline)

			ServiceResponse testResults = new ServiceResponse()
			// Promise
			if (hostOnline) {
                tokenResults = login(netboxClient,rpcConfig)
                if (tokenResults.success) {
				    testResults = testNetworkPoolServer(netboxClient, tokenResults.tokenString, poolServer)
                    if (!testResults.success) {
                        //NOTE invalidLogin was only ever set to false.
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Error calling NetBox').subscribe().dispose()
                    } else {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).subscribe().dispose()
                    }
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'NetBox api not reachable')
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'NetBox api not reachable')
            }
			Date now = new Date()
			if (testResults?.success) {
                String tokenString = tokenResults?.tokenString
				cacheNetworks(netboxClient, tokenString, poolServer)
				if (poolServer?.configMap?.inventoryExisting) {
					cacheIpAddressRecords(netboxClient, tokenString, poolServer)
				}
				morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
			}
		} catch(e) {
			log.error("refresh error: ${e}", e)
		} finally {
            if (tokenResults?.success) {
                logout(netboxClient, rpcConfig, tokenResults.tokenString)
            }
			netboxClient.shutdownClient()
		}
	}

	// cacheNetworks methods
	void cacheNetworks(HttpApiClient client, String tokenString, NetworkPoolServer poolServer, Map opts = [:]) {
		opts.doPaging = true
		def listResults = listNetworks(client, tokenString, poolServer)
		if(listResults.success && listResults.data) {
			List apiItems = listResults.data as List<Map>
            Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
			SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == "${apiItem.id}" && ((apiItem.prefix && ["netboxprefix","netboxprefixipv6"].contains(domainObject.typeCode)) || (!apiItem.prefix && ["netbox","netboxipv6"].contains(domainObject.typeCode)) )
			}.onDelete {removeItems ->
				morpheus.network.pool.remove(poolServer.id, removeItems)
			}.onAdd { itemsToAdd ->
				addMissingPools(poolServer, itemsToAdd)
			}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->

				Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
				return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
					SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
					return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
				}

			}.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
				updateMatchedPools(poolServer, updateItems)
			}.start()
		}
	}

	void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
		List<NetworkPool> missingPoolsList = []
		chunkedAddList?.each { Map it ->
            NetworkPool newNetworkPool
			def networkIp = it.display
            def cidr = it?.start_address ?: it?.prefix
            def startAddress = it?.start_address ? it?.start_address?.tokenize('/')[0] : it?.prefix?.tokenize('/')[0]
            def endAddress = it?.end_address ? it?.end_address?.tokenize('/')[0] : it?.prefix?.tokenize('/')[0]
            def size = it?.size ?: 0
			def rangeConfig
            def addRange
            def poolType
            def name = it?.description ? "${it.description} ${it.display} (Netbox: $poolServer.id)" : "${it?.display} (Netbox: $poolServer.id)"

            log.debug("CIDR: ${cidr} and startAddress: ${startAddress}")

			if(it.family.value == 4) {
                if(it.prefix) {
                    poolType = new NetworkPoolType(code: 'netboxprefix')
                    def networkInfo = getNetworkPoolConfig(it.prefix)
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${it.id}",
                                    cidr: cidr,type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount:networkInfo.config.ipCount,
                                    netmask: NetworkUtility.getNetworkCidrConfig("$cidr").config.netmask]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.setConfigProperty("vrfId", "${it.vrf?.id}".toString())
                    newNetworkPool.ipRanges = []
                    networkInfo.ranges?.each { range ->
                        log.debug("range: ${range}")
                        rangeConfig = [networkPool:newNetworkPool, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:networkInfo.config.ipCount]
                        addRange = new NetworkPoolRange(rangeConfig)
                        newNetworkPool.ipRanges.add(addRange)
                    }
                } else {
                    poolType = new NetworkPoolType(code: 'netbox')
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${it.id}",
                                    cidr: cidr, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount: size,
                                    netmask: NetworkUtility.getNetworkCidrConfig("$cidr").config.netmask]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.setConfigProperty("vrfId", "${it.vrf?.id}".toString())
                    newNetworkPool.ipRanges = []
                    rangeConfig = [cidr:cidr, startAddress:startAddress, endAddress:endAddress, addressCount:size]
                    addRange = new NetworkPoolRange(rangeConfig)
                    newNetworkPool.ipRanges.add(addRange)
                }
            }
            if(it.family.value == 6) {
                if(it.prefix) {
                    poolType = new NetworkPoolType(code: 'netboxprefixipv6')
                    if (startAddress.endsWith(':')) {
                        startAddress = startAddress + '0'
                        endAddress = endAddress + '0'
                    }
                } else {
                    poolType = new NetworkPoolType(code: 'netboxipv6')
                }
                
                def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${it.id}",
                                cidr: cidr, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount: size]
                newNetworkPool = new NetworkPool(addConfig)
                newNetworkPool.setConfigProperty("vrfId", "${it.vrf?.id}".toString())
                newNetworkPool.ipRanges = []
                rangeConfig = [cidrIPv6: cidr, startIPv6Address: startAddress, endIPv6Address: endAddress,addressCount:size]
                addRange = new NetworkPoolRange(rangeConfig)
                newNetworkPool.ipRanges.add(addRange)
			}
			missingPoolsList.add(newNetworkPool)
		}
		morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
	}

	void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
		List<NetworkPool> poolsToUpdate = []
		chunkedUpdateList?.each { update ->
			NetworkPool existingItem = update.existingItem
            Map network = update.masterItem
            def count = network?.size ?: 0

			if(existingItem) {
				//update view ?
				def save = false
				def networkIp = network?.start_address ?: network?.prefix
                def name = network?.description ? "${network.description} ${network.display} (Netbox: $poolServer.id)" : "${network?.display} (Netbox: $poolServer.id)"
                def vrf = "${network?.vrf?.id}".toString()

                if(existingItem?.getConfigProperty("vrfId") != vrf) {
                    existingItem.setConfigProperty("vrfId", vrf)
                    save = true
                }
                if(existingItem?.displayName != name) {
					existingItem.displayName = name
					save = true
				}
				if(existingItem?.cidr != networkIp) {
					existingItem.cidr = networkIp
					save = true
				}
                if(existingItem?.ipCount != count && !network.prefix) {
					existingItem.ipCount = count
					save = true
				}
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
		if(poolsToUpdate.size() > 0) {
			morpheus.network.pool.save(poolsToUpdate).blockingGet()
		}
	}
	
	@Override
	ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
        if (!poolServer.enabled) {
            log.error("Pool server NOT enabled, host record will not be created...")
            return ServiceResponse.error("Pool server NOT enabled, host record will not be created...")
        }

        log.debug("createHostRecord...: $networkPoolIp.ipAddress")

		HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        InetAddressValidator inetAddressValidator = new InetAddressValidator()
        
        def rpcConfig = getRpcConfig(poolServer)
        LoginResult tokenResults = new LoginResult()

        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)

        try {
            tokenResults = login(client, rpcConfig)
            def results = []
            if (tokenResults.success) {
                def hostname = networkPoolIp.hostname
                requestOptions.headers = [Authorization: tokenResults.tokenString]

                if (domain && hostname && !hostname.endsWith(domain.name))  {
                    log.debug("Hostname ${hostname} to have ${domain.name} appended...")
                    hostname = "${hostname}.${domain.name}"
                }

                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId
                def newIpPath
                def tags = []
                def rangeDetails 

                if(poolServer.configMap?.tags) {
                    tags = new JsonSlurper().parseText(addTags(poolServer.configMap?.tags))
				}
                
                if (networkPool.type.code.contains('prefix')) {
                    newIpPath = prefixesPath
                } else {
                    newIpPath = rangesPath
                }

                // get parent IP-range/IP-subnet details, this is required later for IP-address details
                rangeDetails = client.callJsonApi(apiUrl,'/' + newIpPath + networkPool.externalId, requestOptions,'GET')
                log.debug("Parent range details: ${rangeDetails.dump()}")

                if(networkPoolIp.ipAddress) {
                    // Make sure it's a valid IP
                    if (inetAddressValidator.isValidInet4Address(networkPoolIp.ipAddress)) {
                        log.debug("A Valid IPv4 Address Entered: ${networkPoolIp.ipAddress}")
                    } else if (inetAddressValidator.isValidInet6Address(networkPoolIp.ipAddress)) {
                        log.debug("A Valid IPv6 Address Entered: ${networkPoolIp.ipAddress}")
                    } else {
                        log.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}", results)
                        return ServiceResponse.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}")
                    }
                    
                    requestOptions.queryParams = ['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1], 'vrf_id': "${rangeDetails?.data?.vrf?.id}"]
                    // Check IP Usage
                    results = client.callJsonApi(apiUrl,apiPath,requestOptions,'GET')
                    
                    if (results?.success && !results?.error) {
                        if (!results?.data.results) {
                            // If Empty, Create the IP
                            apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                            requestOptions.queryParams = [:]
                            requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],'status':'active',"dns_name":hostname,'tenant':rangeDetails?.data?.tenant?.id,'vrf':rangeDetails?.data?.vrf?.id,'tags':tags ?: []])

                            results = client.callJsonApi(apiUrl,apiPath,requestOptions,'POST')

                        } else if (results?.data?.results){
                            // If Reserved
                            externalId = results.data.results[0].id
                            apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath + externalId + '/'
                            requestOptions.queryParams = [:]
                            requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],'status':'active',"dns_name":hostname,'tenant':rangeDetails?.data?.tenant?.id,'vrf':rangeDetails?.data?.vrf?.id,'tags':tags ?: []])

                            results = client.callJsonApi(apiUrl,apiPath,requestOptions,'PUT')
                        } else {
                            log.error("Allocate IP Error: ${e}", e)
                        }
                    } else {
                        log.error("Allocate IP Error: ${e}", e)
                    }
                } else {
                    // Grab next available IP
                    apiPath = getServicePath(rpcConfig.serviceUrl) + newIpPath
                    requestOptions.queryParams = [:]
                    requestOptions.body = null
                    results = client.callJsonApi(apiUrl, apiPath + networkPool.externalId + '/available-ips/', requestOptions, 'POST')

                    if(results.success && !results.error) {
                        externalId = results.data.id
                        requestOptions.body = JsonOutput.toJson([
                                'address': results.data.address,
                                'status': 'active',
                                "dns_name": hostname,
                                'tenant': rangeDetails?.data?.tenant?.id,
                                'vrf': rangeDetails?.data?.vrf?.id,
                                'tags': tags ?: []
                        ])
                        apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath + externalId + '/'
                        
                        results = client.callJsonApi(apiUrl, apiPath, requestOptions, 'PUT')
                    }
                }

                if (results.success && !results.error) {
                    networkPoolIp.externalId = results.data.id
                    networkPoolIp.ipAddress = results.data.address.tokenize('/')[0]
                    networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    log.warn("API Call Failed to allocate IP Address")
                    return ServiceResponse.error("API Call Failed to allocate IP Address", null, networkPoolIp)
                }
            }
        } catch(e) {
            log.warn("API Call Failed to allocate IP Address {}",e)
            return ServiceResponse.error("API Call Failed to allocate IP Address", null, networkPoolIp)
        } finally {
            if (tokenResults?.success) {
                logout(client,rpcConfig,tokenResults.tokenString)
            }
            client.shutdownClient()
        }
	}

	@Override
	ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
        if (!poolServer.enabled) {
            log.error("Pool server NOT enabled, host record will not be updated...")
            return ServiceResponse.error("Pool server NOT enabled, host record will not be updated...")
        }

        log.debug("updateHostRecord...: $networkPoolIp.ipAddress")
		HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        LoginResult tokenResults = new LoginResult()

        try {
            tokenResults = login(client, rpcConfig)
            def results = []
            def hostname = networkPoolIp.hostname

            if (tokenResults?.success) {
                requestOptions.headers = [Authorization: tokenResults?.tokenString]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId = networkPoolIp.externalId.toString() + '/'

                requestOptions.body = JsonOutput.toJson(['address':networkPoolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1],"dns_name":hostname,'vrf_id': "${networkPool?.getConfigProperty("vrfId")}"])

                results = client.callJsonApi(apiUrl,apiPath + externalId,null,null,requestOptions,'PUT')

                if (results?.success) {
                    return ServiceResponse.success(networkPoolIp)
                } else {
				    return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
			    }
            } else {
                return ServiceResponse.error("Error Authenticating with NetBox",null,networkPoolIp)
            }
        } catch(ex) {
            log.error("Error Updating Host Record {}",ex.message,ex)
            return ServiceResponse.error("Error Updating Host Record ${ex.message}",null,networkPoolIp)
        } finally {
            if (tokenResults?.success) {
                logout(client, rpcConfig, tokenResults?.tokenString)
            }
            client.shutdownClient()
        }
	}

	@Override
	ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords ) {
        if (!networkPool.poolServer.enabled) {
            log.error("Pool server NOT enabled, host record will not be deleted...")
            return ServiceResponse.error("Pool server NOT enabled, host record will not be deleted...")
        }

        log.debug("deleteHostRecord...: $poolIp.ipAddress")

		HttpApiClient client = new HttpApiClient();
        client.networkProxy = morpheusContext.services.setting.getGlobalNetworkProxy()
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        def tokenResults = login(client,rpcConfig)

        try {
            def results = []
            if (tokenResults?.success) {
                requestOptions.headers = [Authorization: tokenResults.tokenString]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
                def externalId = poolIp.externalId.toString() + '/'

                if(poolServer?.configMap?.deprecate){
                    requestOptions.body = JsonOutput.toJson(['address': poolIp.ipAddress + '/' + networkPool.cidr.tokenize('/')[1], "status": "deprecated"])

                    results = client.callJsonApi(apiUrl, apiPath + externalId, null, null, requestOptions, 'PUT')

                    if (results?.success) {
                        return ServiceResponse.success(poolIp)
                    } else {
                        return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, poolIp)
                    }
                } else {
                    results = client.callJsonApi(apiUrl, apiPath + externalId, null, null, requestOptions, 'DELETE')

                    if(!results?.success && (results?.data?.detail == 'Not found.' || results?.data?.detail?.contains('No IPAddress'))) {
                        return ServiceResponse.success(poolIp)
                    } else if (results?.success && !results?.error) {
                        return ServiceResponse.success(poolIp)
                    } else {
                        log.error("Error Deleting Host Record ${poolIp}")
                        return ServiceResponse.error("Error Deleting Host Record ${poolIp}")
                    }
                }
            } else {
                log.error("Error Authenticating with NetBox")
                return ServiceResponse.error("Error Authenticating with NetBox",null,poolIp)
            }
        } catch(x) {
            log.error("Error Deleting Host Record {}",x.message,x)
            return ServiceResponse.error("Error Deleting Host Record ${x.message}",null,poolIp)
        } finally {
            if (tokenResults?.success) {
                logout(client, rpcConfig, tokenResults.tokenString)
            }
            client.shutdownClient()
        }
	}

	private ServiceResponse listNetworks(HttpApiClient client, String tokenString, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def endpoints = [rangesPath, prefixesPath]
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: tokenString]

            endpoints.each{ String ep ->
                def hasMore = true
                def attempt = 0
                def start = 0
                def doPaging = opts.doPaging != null ? opts.doPaging : true
                def maxResults = opts.maxResults ?: 1000
                def apiPath = getServicePath(rpcConfig.serviceUrl) + ep
                log.debug("url: ${apiUrl} path: ${apiPath}")

                if (doPaging == true) {
                    while(hasMore && attempt < 1000) {
                        attempt++

                        requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'status__n':ep == rangesPath ? 'deprecated' : 'container']
                        log.debug("Listing Netbox Network using ${requestOptions.toString()}")
                        def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                        if(results?.success && results?.error != true) {

                            rtn.success = true
                            if(results.data?.results?.size() > 0) {
                                
                                rtn.data += results.data.results

                                if(!results.data.next) {
                                    hasMore = false
                                } else {
                                    start += maxResults
                                }
                
                            } else {
                                hasMore = false
                            }
                        } else {
                            hasMore = false

                            if(!rtn.success) {
                                rtn.msg = results.error
                            }
                        }
                    }
                } else {
                    requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString(),'status__n':ep == rangesPath ? 'deprecated' : 'container']
                    log.info("Listing Netbox Network using: ${requestOptions.headers.toString()}: ${requestOptions.queryParams.toString()}")
                    def results = client.callJsonApi(apiUrl, apiPath, null, null, requestOptions, 'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.size() > 0) {
                            rtn.data = results.data.results
                        }
                    } else {
                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            }
        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }
        log.debug("List Networks Results: ${rtn}")
        return rtn
	}

	// cacheIpAddressRecords
    void cacheIpAddressRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).concatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.concatMap { NetworkPool pool ->
            def listResults = listHostRecords(client,token,poolServer,pool)
            if (listResults.success) { // && listResults.data) {  //FIX HERE, makes it fail if there is only 1 IP, needs only success, not data also
                List<Map> apiItems = listResults.data
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == "${apiItem.id}"
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem.address.tokenize('/')[0]
                }.onDelete {removeItems ->
                    removeItems.each {
                        removeRelatedIps(it, pool)
                    }
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems, poolServer)
                }.observe()
            } else {
                return Single.just(false).toObservable()
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.blockingSubscribe()
    }


    void removeRelatedIps(NetworkPoolIpIdentityProjection removeItem, NetworkPool pool) {
        //related IP could be in IP that is in both a prefix and a pool
        log.debug("Remove related for IP: $removeItem.ipAddress")
        NetworkPoolIp item = morpheusContext.network.pool.poolIp.get(removeItem.id).blockingGet()
        //multiple ip records may exist, one for pool, one for range...
        List<NetworkPoolIp> ipList = morpheusContext.network.pool.poolIp.list(new DataQuery()
                .withFilter('externalId', item.externalId)
                .withFilter('ipAddress', item.ipAddress)
                .withFilter('id', '!=', item.id)
        ).toList().blockingGet()

        def dupIsInSameIntegration = ipList.findAll {
            def resultPool = morpheusContext.network.pool.get(it.networkPool.id).blockingGet()
            def itemPool = morpheusContext.network.pool.get(item.networkPool.id).blockingGet()
            //looking for duplicate IPs that belong to the same integration(poolServer) and share the same VRF
            return resultPool.poolServer.id == itemPool.poolServer.id && it.networkPool.getConfigProperty("vrfId") == item.networkPool.getConfigProperty("vrfId") //ipToVrf["$it.externalId"] == ipToVrf["$item.externalId"]
        }

        dupIsInSameIntegration.each {
            try {
                log.debug("Found duplicate IP  for $removeItem.id for remove: $it.ipAddress($it.id). Pool: $it.networkPool.name, $it.networkPool.id")
                morpheusContext.network.pool.poolIp.remove(it.networkPool.id, [it]).blockingGet()
                //ipToVrf.remove(it.externalId)
            } catch (Exception e) {
                log.error("Error removing IP: $e")
            }
        }
    }


	void addMissingIps(NetworkPool pool, List addList) {
        addList?.each { it ->
			def ipAddress = it.address.tokenize('/')[0]
			def types = it.status.value
			def ipType = 'assigned'
			if(types == 'reserved') {
				ipType = 'reserved'
			} else if (types == 'deprecated') {
                ipType = 'unmanaged'
            }
            try {
                def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: it.dns_name, ipAddress: ipAddress, externalId:it.id]
                def newObj = new NetworkPoolIp(addConfig)
                morpheus.network.pool.poolIp.create(pool, [newObj]).blockingGet()
            } catch (Exception e) {
                // don't break if one IP fails (probably due to netbox allowing duplicates)
                log.error("Error adding IP: $e")
            }
		}
	}


	void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList, NetworkPoolServer poolServer) {
		List<NetworkPoolIp> ipsToUpdate = []
		updateList?.each {  update ->
			NetworkPoolIp existingItem = update.existingItem

			if(existingItem) {
                def oldHostname = (existingItem?.hostname ?: "").toString().trim()
                def newHostname = (update.masterItem.dns_name ?: "").toString().trim()
                def types = update.masterItem.status.value
                //def vrfId = "${update.masterItem.vrf?.id}"
				def ipType = 'assigned'
                if(types == 'reserved') {
                    ipType = 'reserved'
                } else if (types == 'deprecated') {
                    ipType = 'unmanaged'
                }
				def save = false

                if(existingItem.ipType != ipType) {
					existingItem.ipType = ipType
					save = true
				}
				if(oldHostname != newHostname) {
					existingItem.hostname = newHostname
					save = true
				}
				if(save) {
					ipsToUpdate << existingItem
				}
			}
		}
		if(ipsToUpdate.size() > 0) {
			morpheus.network.pool.poolIp.save(ipsToUpdate).blockingGet()
		}
	}


	private ServiceResponse listHostRecords(HttpApiClient client, String tokenString, NetworkPoolServer poolServer,NetworkPool networkPool, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: tokenString]

            def startAddress = networkPool?.ipRanges?[0].startAddress ?: null
            def endAddress = networkPool?.ipRanges?[0].endAddress ?: null
            def hasMore = true
            def attempt = 0
            def start = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def maxResults = opts.maxResults ?: 1000
            def apiPath = getServicePath(rpcConfig.serviceUrl) + getIpsPath
            boolean savePool = false

            def vrfId = networkPool.getConfigProperty("vrfId") ?: "null"
            log.debug("List Netbox Hosts: Request URL: $apiUrl, path: $apiPath, pool: ${networkPool.cidr}, VRF: $vrfId")

            if (doPaging == true) {
                while(hasMore && attempt < 1000) {
                    attempt++

                    requestOptions.queryParams = [limit: maxResults.toString(), offset: start.toString(), 'parent': networkPool.cidr.toString(), 'vrf_id': "$vrfId"]
                    def results = client.callJsonApi(apiUrl, apiPath, null, null, requestOptions, 'GET')
                    log.debug("Netbox Pool API Call: $apiPath, $requestOptions.queryParams")

                    if (results?.success && !results?.error) {
                        rtn.success = true
                        if (results.data?.results?.size() > 0) {
                            results.data.results.each { it ->

                                def ip = it.address.tokenize('/')[0]
                                if (!startAddress || !endAddress || isIPInRange(ip, startAddress, endAddress)) {
                                    if (hasTagByName(it, "$poolServer.configMap.gwTag")) {
                                        networkPool.gateway = it.address.split("/")[0]
                                        savePool = true
                                    }
                                    rtn.data << it
                                }
                            }

                            if(!results.data.next) {
                                hasMore = false
                            } else {
                                start += maxResults
                            }
            
                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            } else {
                requestOptions.queryParams = [limit: maxResults.toString(), offset: start.toString(), 'parent': networkPool.cidr.toString(), 'vrf_id': vrfId]
                def results = client.callJsonApi(apiUrl, apiPath, null, null, requestOptions, 'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.size() > 0) {
                        rtn.data = results.data.results
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
            if (savePool) {
                morpheusContext.network.pool.save(networkPool).blockingGet()
            }
        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }
        log.debug("List Networks Results: ${rtn}")
        return rtn
	}


	ServiceResponse testNetworkPoolServer(HttpApiClient client, String tokenString, NetworkPoolServer poolServer) {
		def rtn = new ServiceResponse()
		try {
			def opts = [doPaging:false, maxResults:1]
			def networkList = listNetworks(client, tokenString, poolServer, opts)
			rtn.success = networkList.success
			rtn.data = [:]
			if(!networkList.success) {
				rtn.msg = 'error connecting to NetBox'
			}
		} catch(e) {
			rtn.success = false
			log.error("test network pool server error: ${e}", e)
		}
		return rtn
	}


	/**
	 * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
	 * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
	 */
	Collection<NetworkPoolType> getNetworkPoolTypes() {
		return [
			new NetworkPoolType(code:'netbox', name:'NetBox', creatable:false, description:'NetBox', rangeSupportsCidr: false),
			new NetworkPoolType(code:'netboxipv6', name:'NetBox IPv6', creatable:false, description:'NetBox IPv6', rangeSupportsCidr: true, ipv6Pool:true),
            new NetworkPoolType(code:'netboxprefix', name:'NetBox Prefix', creatable:false, description:'NetBox Prefix', rangeSupportsCidr: false),
			new NetworkPoolType(code:'netboxprefixipv6', name:'NetBox Prefix IPv6', creatable:false, description:'NetBox Prefix IPv6', rangeSupportsCidr: true, ipv6Pool:true)
		]
	}

	/**
	 * Provide custom configuration options when creating a new {@link AccountIntegration}
	 * @return a List of OptionType
	 */
	@Override
	List<OptionType> getIntegrationOptionTypes() {
		return [
				new OptionType(code: 'netbox.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x/', displayOrder: 0, required:true),
				new OptionType(code: 'netbox.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local', optionSource: 'credentials',
                    config: '{"credentialTypes":["username-password","api-key"]}', helpText: "Username + Password <or> Token Only if using Local Credentials"),

				new OptionType(code: 'netbox.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2,localCredential: true, required: false),
				new OptionType(code: 'netbox.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3,localCredential: true, required: false),
                new OptionType(code: 'netbox.apiToken', name: 'API Token', inputType: OptionType.InputType.TEXT, fieldName: 'apiToken', fieldLabel: 'API Token', fieldContext: 'config', displayOrder: 4,localCredential: true, helpText: "For v2 token, use nbt_<key>.<token> format."),
				new OptionType(code: 'netbox.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 5),
				new OptionType(code: 'netbox.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 6),
				new OptionType(code: 'netbox.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 7),
                new OptionType(code: 'netbox.deprecate', name: 'Deprecate on Delete', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'deprecate', fieldLabel: 'Deprecate on Delete', fieldContext: 'config', displayOrder: 8),
                new OptionType(code: 'netbox.v2token', name: 'Use v2 Tokens', inputType: OptionType.InputType.CHECKBOX, defaultValue: 1, fieldName: 'v2token', fieldLabel: 'Use v2 Tokens', fieldContext: 'config', displayOrder: 9),
                new OptionType(code: 'netbox.tags', name: 'Tags', inputType: OptionType.InputType.TEXT, fieldName: 'tags', fieldLabel: 'Tags', fieldContext: 'config', displayOrder: 10, helpText: "value|value2"),
                new OptionType(code: 'netbox.gwTag', name: 'Gateway Tag', inputType: OptionType.InputType.TEXT, fieldName: 'gwTag', fieldLabel: 'Gateway Tag', fieldContext: 'config', displayOrder: 11, helpText: "Populate as gateway if tag found. This gateway will take priority over the gateway specified in the network configuration if used."),
		]
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"netbox.svg", darkPath: "netbox.svg")
	}


    class LoginResult {
        Boolean success = false
        String tokenString
    }


    LoginResult login(HttpApiClient client, rpcConfig) {
        log.debug("Running login with v2 token value of: $rpcConfig.v2token")
        def rtn = new LoginResult()
        String tokenPrefix = rpcConfig?.v2token ? "Bearer " : "Token "

        if (!rpcConfig.username) {
            if (!rpcConfig.password) {
                log.error("getToken error: No user name, so missing token or password")
            } else {
                rtn.tokenString = tokenPrefix + rpcConfig.password.toString()
                //log.debug("TokenString: $rtn.tokenString")
                rtn.success = true
            }
            return rtn
        } else if (!rpcConfig.password) {
            log.error("getToken error: missing password")
        }

        try {
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = ['content-type':'application/json']
            requestOptions.body = JsonOutput.toJson([
                    username: rpcConfig.username,
                    password: rpcConfig.password,
                    expires: formatDate(getCurrentTimePlus5Minutes()),
                    description: 'Generated by Morpheus'
            ])

            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + authPath

            def results = client.callJsonApi(apiUrl, apiPath, requestOptions, 'POST')
            if (results?.success && !results?.error) {
                log.debug("login: ${results}")
                log.info("rpcConfig: $rpcConfig")
                if (rpcConfig?.v2token) {
                    rtn.tokenString = "Bearer nbt_${results.data?.key?.trim()}.${results.data?.token?.trim()}"
                } else {
                    rtn.tokenString = "Token ${results.data?.key?.trim()}"
                }
                rtn.success = true
            } else {
                log.error("getToken error: ${results.toMap()}")
            }
        } catch (e) {
            log.error("getToken error: ${e}", e)
        }

        return rtn
    }


    void logout(HttpApiClient client, rpcConfig, String tokenString) {
        try {
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'logout'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: tokenString]
            client.callJsonApi(apiUrl,apiPath,requestOptions,"GET")
        } catch(e) {
            log.error("logout error: ${e}", e)
        }
    }


    private getRpcConfig(NetworkPoolServer poolServer) {
        return [
            username: poolServer.credentialData?.username ?: poolServer.serviceUsername,
            password: poolServer.credentialData?.password ?: poolServer.servicePassword ?: poolServer.configMap?.apiToken,
            serviceUrl: poolServer.serviceUrl,
            ignoreSSL: poolServer.ignoreSsl,
            v2token: poolServer.configMap?.v2token
        ]
    }


	private static String cleanServiceUrl(String url) {
		def rtn = url
		def slashIndex = rtn?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = rtn.substring(0, slashIndex)
		return rtn
	}


	private static String getServicePath(String url) {
		def rtn = '/'
		def slashIndex = url?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = url.substring(slashIndex)
		if(rtn?.endsWith('/'))
			return rtn.substring(0, rtn.lastIndexOf("/"));
		return rtn
	}


    static Map getNetworkPoolConfig(String cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }


    def isIPInRange(ipAddress, startRange, endRange) {
        try {
            def start = InetAddress.getByName(startRange)
            def end = InetAddress.getByName(endRange)
            def ip = InetAddress.getByName(ipAddress)

            log.debug("StartIP: ${start}, endAddress: ${end}, ip: ${ip}")

            // Check for IPv4 or IPv6
            if (ip instanceof Inet4Address && start instanceof Inet4Address && end instanceof Inet4Address) {
                // IPv4
                // def ipInt = (ip.getAddress()[0] << 24) | (ip.getAddress()[1] << 16) | (ip.getAddress()[2] << 8) | ip.getAddress()[3]
                // def startInt = (start.getAddress()[0] << 24) | (start.getAddress()[1] << 16) | (start.getAddress()[2] << 8) | start.getAddress()[3]
                // def endInt = (end.getAddress()[0] << 24) | (end.getAddress()[1] << 16) | (end.getAddress()[2] << 8) | end.getAddress()[3]
                def ipInt = ipv4ToInteger(ip.toString())
                def endInt = ipv4ToInteger(end.toString())
                def startInt = ipv4ToInteger(start.toString())
                log.debug("startInt: ${startInt}, endInt: ${endInt}, ipInt: ${ipInt}")
                return ipInt >= startInt && ipInt <= endInt
            } else if (ip instanceof Inet6Address && start instanceof Inet6Address && end instanceof Inet6Address) {
                // IPv6
                def ipBytes = ip.getAddress()
                def startBytes = start.getAddress()
                def endBytes = end.getAddress()
                return ipBytes >= startBytes && ipBytes <= endBytes
            } else {
                // Invalid IP version
                return false
            }
        } catch (e) {
            // Handle invalid IP address
            log.error("cacheIpAddress  error: ${e}", e)
        }
    }

    def addTags(String tags) {
        // Split the string into individual values using '|'
        def tagValues = tags.split("\\|")

        // Create a list of JSON objects
        def tagObjects = tagValues.collect { value ->
            [
                name: value
            ]
        }

        def jsonTags = JsonOutput.toJson(tagObjects)

        return jsonTags
    }

    def getCurrentTimePlus5Minutes() {
        Calendar calendar = Calendar.getInstance()
        calendar.add(Calendar.MINUTE, 5)

        return calendar.time
    }

    def formatDate(Object date, String outputFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") {
        def rtn
        try {
            if(date) {
                if(date instanceof Date)
                    rtn = date.format(outputFormat, TimeZone.getTimeZone('GMT'))
                else if(date instanceof CharSequence)
                    rtn = date
            }
        } catch(ignored) { }
        return rtn
    }

    def ipv4ToInteger(ipAddress) {
        if (ipAddress.startsWith("/")) {
            ipAddress = ipAddress.substring(1)
        }
        def parts = ipAddress.split('\\.') // Split the IP address into octets

        def result = 0
            for (int i = 0; i < 4; i++) {
                def octet = parts[i].toInteger()
                result = (result << 8) | octet
            }

        return result
    }

    static boolean hasTagByName(Map body, String tagName) {
        def tags = body?.tags
        if (!(tags instanceof List)) return false

        return tags.any { it?.name == tagName }
    }
}

'use strict';

const Client = require('azure-iot-device').Client;
const Protocol = require('azure-iot-device-mqtt').Mqtt;
const azureStorage = require('azure-storage');
const unzip = require("unzip");
const fs = require('fs');
const ncp = require('ncp').ncp;
const path = require('path');
const mkdirp = require('mkdirp');
const Message = require('azure-iot-device').Message;
const consts = require('./constants.js');
const Utilities = require('./utilities.js');

var utilities = null;
var jobCount = 1;
var deviceShutDown = false;
var MetricsToRetry = [];
var metricsRetryFrequency = 30;

class iothubdt {
    constructor() {
        this.iothub_client = null;
        this.blobSvc = null;
        this.connected = false;
        this.startTime = new Date();
        this.output = null;
        this.twin = null;
        this.path = null;
        this.patch = {};
    }

    /**
     * @description     initilize connection to iothub
     *
     * @param  err  error from set up connection
     *
     */
    on_connect(err) {
        if (err) {
            utilities.log('[Client] could not open IotHub client');
            this.connected = false;
        } else {
            utilities.log('[Client] client opened');
            this.iothub_client.getTwin(function (err, twin) {
                if (err) {
                    utilities.log('[Client] could not get twin');
                    this.connected = false;
                } else {
                    this.twin = twin;
                    this.printDeviceTwinStatus(twin);
                    try {
                        twin.on('properties.desired', (delta) => {
                            utilities.log(`[Client] new desired properties received:${JSON.stringify(delta)}`);
                            this.twinStopJobHandler(delta);
                        });
                    } catch (err) {
                        this.connected = false;
                        utilities.log(err.toString());
                    }
                }
            }.bind(this));

            /// Device listen on a method
            try {
                this.iothub_client.onDeviceMethod(consts.MethodEnum.StartJobDirectMethodName, this.onStartJob.bind(this));
                this.iothub_client.onDeviceMethod(consts.MethodEnum.StopJobDirectMethodName, this.onStopJob.bind(this));
                this.iothub_client.onDeviceMethod(consts.MethodEnum.PingDirectMethodName, this.onPing.bind(this));
                this.iothub_client.onDeviceMethod(consts.MethodEnum.UpdateJobDirectMethodName, this.onUpdateJob.bind(this));
                this.iothub_client.onDeviceMethod(consts.MethodEnum.ShutDownDeviceDirectMethodName, this.onShutDownDevice.bind(this));
                this.connected = true;
            } catch (err) {
                this.connected = false;
                utilities.log(err.toString());
            }
        }
        setTimeout(() => { this.deviceRestartHandler(); }, 3000);
    }

    /**
    * @description      Create function.
    *
    * @param  messageBus   gateway messagebus initilization.
    * @param  configuration  gateway configuration initilization.
    *
    * @return boolean   Indicte whether module initilize properly.
    */
    create(messageBus, configuration) {
        this.messageBus = messageBus;
        this.configuration = configuration;
        this.dataSentWhenStoppedTag = this.configuration.data_sent_when_stopped;
        this.dataSentWhenRunningTag = this.configuration.data_sent_when_running;

        utilities = new Utilities(this.messageBus);
        var that = this;

        setInterval(() => {
            var lenthForThisRun = MetricsToRetry.length;
            for (var i = 0; i < lenthForThisRun; i++) {
                var item = MetricsToRetry.shift();
                var recordTime = new Date(item.properties['recordtime']);
                if ((new Date) - recordTime > consts.metricsDiscardLatency) {
                    utilities.log("[Metric] Discard metrics 20 mins late");
                    continue;
                }

                that.iothub_client.sendEvent(item, err => {
                    if (err) {
                        utilities.log(`[Metric] Failed to resend metrics to IoT Hub: ${err.toString()}`);
                        MetricsToRetry.push(item);
                    }
                    else {
                        utilities.log("[Metric] Retry: Successfully Send metrics data to iothub");
                    }
                });
            }
        }, metricsRetryFrequency * 1000);

        if (this.configuration && this.configuration.connection_string) {
            /// open a connection to the IoT Hub
            try {
                this.iothub_client = Client.fromConnectionString(this.configuration.connection_string, Protocol);
                this.iothub_client.open(this.on_connect.bind(this));
            } catch (err) {
                this.connected = false;
                utilities.log(err.toString());
            }
        }
        else {
            utilities.log('[Client] This module requires the connection string to be passed in via configuration.');
        }

        setInterval(() => {
            if (this.connected === false) {
                this.iothub_client = Client.fromConnectionString(this.configuration.connection_string, Protocol);
                this.iothub_client.open(this.on_connect.bind(this));
            }
        }, consts.clientConnectionCheckFrequency);

        return true;
    }

    /**
    * @description     Receive message from ASA module.
    *
    * @param  message   message from messgae bus.
    *
    */
    receive(message) {
        try {
            var buf = new Buffer(message.content);
            var receive = buf.toString();
            if (message.properties.type === "Status") {
                if (this.connected === true) {
                    if (this.twin) {
                        this.deviceTwinReport(message.properties.jobid,
                            message.properties.jobrunid,
                            message.properties.instanceid,
                            message.properties.status,
                            message.properties.version,
                            receive);
                    }
                }
            }
            else if (message.properties.type === "ASAMetrics") {
                if (this.connected === true) {
                    var m = new Message(receive);
                    if (message.properties) {
                        for (var prop in message.properties) {
                            m.properties.add(prop, message.properties[prop]);
                        }
                    }

                    this.iothub_client.sendEvent(m, err => {
                        if (err) {
                            utilities.log(`[Metric] An error occurred when sending message to Azure iothub: ${err.toString()}`);
                            MetricsToRetry.push(m);
                        }
                        else {
                            utilities.log("[Metric] Successfully send metrics data to iothub");
                        }
                    });
                }
            } 
            else if (this.dataSentWhenStoppedTag && message.properties.name === this.dataSentWhenStoppedTag) {
                if (this.connected === true) {
                    var m = new Message(receive);
                    if (message.properties) {
                        for (var prop in message.properties) {
                            m.properties.add(prop, message.properties[prop]);
                        }
                    }

                    this.iothub_client.sendEvent(m, err => {
                        if (err) {
                            utilities.log(`An error occurred when sending message to Azure IoT Hub: ${err.toString()}`);
                        }
                    });
                }
            } 
            else if (this.dataSentWhenRunningTag && message.properties.name === this.dataSentWhenRunningTag) {
                if (this.connected === true) {
                    var m = new Message(receive);
                    if (message.properties) {
                        for (var prop in message.properties) {
                            m.properties.add(prop, message.properties[prop]);
                        }
                    }

                    this.iothub_client.sendEvent(m, err => {
                        if (err) {
                            utilities.log(`An error occurred when sending message to Azure IoT Hub: ${err.toString()}`);
                        }
                    });
                }
            }
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
     * Device twin reported status:
     * * [Initializing] Device receives job successfully and prepare for the job run. 
     * * [Processing] the Inputs and Outputs in ASA module are both more than 0
     * * [Idle]  the Inputs and Outputs in ASA module equals to 0
     * * [Degraded] When it catches exceptions in ASA module for a Job 
     * * [Stopped] Device stops accepting inputs and stop producing metrics. 
     * * [Failed] Device catch exceptions, which indicates unable to process data events
     */
    deviceTwinReport(jobid, jobrunid, instanceid, status, version, diagnostic) {
        var jobrunidarray = {};
        jobrunidarray.status = status;
        jobrunidarray.diagnostic = diagnostic;
        jobrunidarray.jobRunId = jobrunid;
        jobrunidarray.instanceId = instanceid;
        jobrunidarray.runtimeVersion = version;
        this.patch[consts.deviceTwinEdgeSessionName] = {};
        this.patch[consts.deviceTwinEdgeSessionName][jobid] = jobrunidarray;
        /// reported properties update
        this.twin.properties.reported.update(this.patch, (err) => {
            if (err) {
                if (deviceShutDown === false) {
                    utilities.log('[Client] could not update twin, reinitilize IOT client');
                    this.iothub_client = Client.fromConnectionString(this.configuration.connection_string, Protocol);
                    this.iothub_client.open(this.on_connect.bind(this));
                }
            } else {
                utilities.log('[Client] twin state reported');
                utilities.log(JSON.stringify(this.patch));
            }
        });
    }

    /**
    * @description     Clean up previous job infomation in twin reported property.
    *
    */
    cleanTwin() {
        this.patch[consts.deviceTwinEdgeSessionName] = null;
        this.twin.properties.reported.update(this.patch, (err) => {
            if (err) {
                utilities.log('[Client] Could not clean up device twin');
            } else {
                utilities.log('[Client] Finished cleaning up of device twin');
            }
        });
    }

    /**
     * @description  Device subscript to desired property change in Twin and handle the stopped command
     * 
     * @param  desiredJobInfo  The desired 
     * 
     */
    twinStopJobHandler(desiredJobInfo) {
        try {
            utilities.log(`[Client] Receive Twin stop notification: ${JSON.stringify(desiredJobInfo)}`);
            var filePath = path.join(__dirname, consts.FolderFileNameEnum.JobRunCheckpointFile);
            if (!desiredJobInfo.hasOwnProperty(consts.deviceTwinEdgeSessionName) || desiredJobInfo[consts.deviceTwinEdgeSessionName] == null) {
                return;
            }
            desiredJobInfo = desiredJobInfo[consts.deviceTwinEdgeSessionName];
            Object.keys(desiredJobInfo).forEach((val) => {
                if (desiredJobInfo[val].hasOwnProperty("status") &&
                    desiredJobInfo[val]["status"] === "Stopped") {
                    if (fs.existsSync(filePath)) {
                        fs.readFile(filePath, 'utf8', (error, data) => {
                            var runningJobs = JSON.parse(data);
                            if (runningJobs.hasOwnProperty(val)) {
                                this.deleteLocalFile(val, consts.FolderFileNameEnum.JobRunCheckpointFile);
                                utilities.modifyLocalFile(val, runningJobs[val]["jobRunId"], runningJobs[val]["instanceId"], null, consts.MethodEnum.StopJobDirectMethodName, consts.FolderFileNameEnum.LastUserAction);
                                this.sendQueryMessage(val, runningJobs[val]["jobRunId"], runningJobs[val]["instanceId"], null, consts.MethodEnum.StopJobDirectMethodName, consts.emptycontent);
                            }
                        });
                    }
                }
            });
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Handler for ping request. This request is used to check connectivity of device.
    *
    * @param  request   request object contains header and payload.
    * @param  response  response object contains HttpStatusCode and payload.
    *
    */
    onPing(request, response) {
        utilities.log(`[Client] receive Ping Request: ${request.payload}`);
        response.send(200, "Connected", (err) => {
            if (err) {
                utilities.log(`An error ocurred when sending a method response:\n ${err.toString()}`);
            } else {
                utilities.log(`Response 200 to ${request.methodName} sent successfully.`);
            }
        });
    }

    /**
    * @description      Handler for shut Down device request. This request is to disconnect the client on device
    *
    * @param  request   request object contains header and payload.
    * @param  response  response object contains HttpStatusCode and payload.
    *
    */
    onShutDownDevice(request, response) {
        utilities.log("[Client receive device shut down Request]");
        response.send(200, "Connected", function (err) { });
        this.iothub_client.close(err => {
            if (err) {
                utilities.log(`An error occurred when disconnecting from Azure IoT Hub: ${err.toString()}`);
            }
        });
        deviceShutDown = true;

        setTimeout(() => {
            deviceShutDown = false;
            this.iothub_client = Client.fromConnectionString(this.configuration.connection_string, Protocol);
            this.iothub_client.open(this.on_connect.bind(this));
        }, 200 * 1000);
    }

    /**
    * @description      Handler for stop job request.
    *
    * @param  request   request object contains header and payload.
    * @param  response  response object contains HttpStatusCode and payload.
    *
    */
    onStopJob(request, response) {
        utilities.log(`[Client] receive Stop Request: ${JSON.stringify(request.payload)}`);
        this.validateRequestPayload(request.payload, request.methodName, (error) => {
            if (error) {
                response.send(400, consts.FatalErrorEnum.InvalidRequestPayloadFormat, (err) => {
                    if (err) {
                        utilities.log(`[Client] An error ocurred when sending a method response:${err.toString()}`);
                    } else {
                        utilities.log(`[Client] Response 400 to ${request.methodName} sent successfully.`);
                    }
                });
            } else {
                this.checkJobRunExist(request.payload, (exist) => {
                    utilities.log(`[Client] check job run exist : ${exist}`);
                    if (exist === true) {
                        response.send(200, "Received", (err) => {
                            if (err) {
                                utilities.log(`An error ocurred when sending a method response:\n ${err.toString()}`);
                            } else {
                                utilities.log(`Response 200 to ${request.methodName} sent successfully.`);
                            }
                        });
                        this.deleteLocalFile(request.payload.jobId, consts.FolderFileNameEnum.JobRunCheckpointFile);
                        utilities.modifyLocalFile(request.payload.jobId, null, null, null, request.methodName, consts.FolderFileNameEnum.LastUserAction);
                        this.sendQueryMessage(request.payload.jobId, request.payload.jobRunId, request.payload.instanceId, null, request.methodName, consts.emptycontent);
                    } else if (exist === false) {
                        response.send(404, consts.FatalErrorEnum.NoSuchJobRunningOnDevice, (err) => {
                            if (err) {
                                utilities.log(`An error ocurred when sending a method response:\n ${err.toString()}`);
                            } else {
                                utilities.log(`Response 404 to ${request.methodName} sent successfully.`);
                            }
                        });
                    }
                });
            }
        });
    }

    /**
    * @description      Handler for start job request.
    *
    * @param  request   request object contains header and payload.
    * @param  response  response object contains HttpStatusCode and payload.
    *
    */
    onStartJob(request, response) {
        utilities.log(`[Client] receive Start Request: ${JSON.stringify(request.payload)}`);
        // clean up the twin reported properties
        this.cleanTwin();
        var query = null;
        var userdefinedjobinfo = null;
        var config = null;
        var errors = [];
        var checkdownloaderror = true;
        var downloadFileCount = Object.keys(request.payload).length - 1;
        this.validateRequestPayload(request.payload, request.methodName, (error) => {
            if (error) {
                response.send(400, consts.FatalErrorEnum.InvalidRequestPayloadFormat, (err) => {
                    if (err) {
                        var errorMessage = '[Client] An error ocurred when sending a method response:\n' + err.toString();
                        utilities.log(errorMessage);
                    } else {
                        utilities.log(`[Client] Response 400 to ${request.methodName} sent successfully.`);
                    }
                });
            } else {
                this.checkJobCountFull((full) => {
                    utilities.log(`[Client] check job count full: ${full}`);
                    if (full === true) {
                        response.send(409, consts.FatalErrorEnum.JobConflictOnDevice, (err) => {
                            if (err) {
                                var errorMessage = '[Client] An error ocurred when sending a method response:\n' + err.toString();
                                utilities.log(errorMessage);
                            } else {
                                utilities.log(`[Client] Response 409 to ${request.methodName} sent successfully.`);
                            }
                        });
                    } else {
                        response.send(202, "Received", (err) => {
                            if (err) {
                                var errorMessage = '[Client] An error ocurred when sending a method response:\n' + err.toString();
                                utilities.log(errorMessage);
                            } else {
                                utilities.log(`[Client] Response 202 to ${request.methodName} sent successfully.`);
                            }
                        });
                    }
                });
            }
            this.sendQueryMessage(request.payload.jobId, null, null, null, null, consts.emptycontent);
        });
        try {
            this.downloadFromBlob(request.payload, (error, type, payload) => {
                checkdownloaderror = checkdownloaderror && error;
                switch (type) {
                    case consts.BlobDownloadEnum.JobInfo:
                        var jobinfos = fs.readFileSync(path.join(__dirname, consts.BlobFileNamesEnum.JobInfo), 'utf8').split(",");
                        var jsonconfig = JSON.parse(jobinfos);
                        config = {};
                        jsonconfig.map((val) => {
                            var keyvalue = val.split("=");
                            utilities.log(`${keyvalue[0]} : ${keyvalue[1]}`);
                            config[keyvalue[0]] = keyvalue[1];
                        });
                        utilities.log("[Client] Get Jobinfo");
                        break;
                    case consts.BlobDownloadEnum.TstreamsBits:
                        utilities.log("[Client] Get Tstream");
                        break;
                    case consts.BlobDownloadEnum.UserdefinedJobInfo:
                        utilities.log("[Client] Get User Defined Job Info");
                        userdefinedjobinfo = payload;
                        break;
                    case consts.BlobDownloadEnum.Error:
                        errors.push(payload);
                        break;
                }
                if (--downloadFileCount == 0) {
                    if (checkdownloaderror === false) {
                        var errorMessage = "[Client] Read From Blob Error: " + errors.toString();
                        utilities.log(errorMessage);
                        if (config) {
                            this.deviceTwinReport(request.payload.jobId, config.jobrunid, config.instanceid, consts.DeviceJobStatusEnum.Failed, config.version, errorMessage);
                        } else {
                            this.deviceTwinReport(request.payload.jobId, null, null, consts.DeviceJobStatusEnum.Failed, null, errorMessage);
                        }
                        utilities.modifyLocalFile(request.payload.jobId, null, null, null, consts.DeviceJobStatusEnum.Failed, consts.FolderFileNameEnum.StatusFile);
                        utilities.modifyLocalFile(request.payload.jobId, null, null, null, errorMessage, consts.FolderFileNameEnum.FailureError);
                    } else {
                        utilities.log("[Client] Read From Blob Succeed");
                        this.writeToDisk(config, userdefinedjobinfo, request.methodName);
                        this.sendQueryMessage(config.jobid, config.jobrunid, config.instanceid, config.version, request.methodName, userdefinedjobinfo);
                        this.folderReorg(config);
                    }
                }
            });
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Handler for update job request.
    *
    * @param  request   request object contains header and payload.
    * @param  response  response object contains HttpStatusCode and payload.
    *
    */
    onUpdateJob(request, response) {
        utilities.log(`[Client] receive Update Request: ${JSON.stringify(request.payload)}`);
        // clean up the twin reported properties
        var config = null;
        var query = null;
        var userdefinedjobinfo = null;
        var errors = [];
        var checkdownloaderror = true;
        var downloadFileCount = Object.keys(request.payload).length - 2;
        this.validateRequestPayload(request.payload, request.methodName, (error) => {
            if (!!error) {
                response.send(400, consts.FatalErrorEnum.InvalidRequestPayloadFormat, (err) => {
                    if (err) {
                        var errorMessage = '[Client] An error ocurred when sending a method response:\n' + err.toString();
                        utilities.log(errorMessage);
                    } else {
                        utilities.log(`Response 400 to ${request.methodName} sent successfully.`);
                    }
                });
            } else {
                response.send(202, "Received", (err) => {
                    if (err) {
                        var errorMessage = '[Client] An error ocurred when sending a method response:\n' + err.toString();
                        utilities.log(errorMessage);
                    } else {
                        utilities.log(`[Client] Response 202 to ${request.methodName} sent successfully.`);
                    }
                });

                try {
                    this.downloadFromBlobUpgrade(request.payload, (error, type, payload) => {
                        checkdownloaderror = checkdownloaderror && error;
                        switch (type) {
                            case consts.BlobDownloadEnum.JobInfo:
                                var jobinfos = fs.readFileSync(path.join(__dirname, consts.BlobFileNamesEnum.JobInfo), 'utf8').split(",");
                                var jsonconfig = JSON.parse(jobinfos);
                                config = {};
                                jsonconfig.map((val) => {
                                    var keyvalue = val.split("=");
                                    utilities.log(`${keyvalue[0]} : ${keyvalue[1]}`);
                                    config[keyvalue[0]] = keyvalue[1];
                                });
                                utilities.log("[Client] Get Jobinfo");
                                break;
                            case consts.BlobDownloadEnum.TstreamsBits:
                                utilities.log("[Client] Get Tstream");
                                break;
                            case consts.BlobDownloadEnum.UserdefinedJobInfo:
                                utilities.log("[Client] Get User Defined Job Info");
                                userdefinedjobinfo = payload;
                                break;
                            case consts.BlobDownloadEnum.Error:
                                errors.push(payload);
                                break;
                        }
                        if (--downloadFileCount == 0) {
                            if (checkdownloaderror === false) {
                                var errorMessage = "[Client] Read From Blob Error: " + errors.toString();
                                utilities.modifyLocalFile(config.jobid, config.jobrunid, config.instanceid, config.version, userdefinedjobinfo, consts.FolderFileNameEnum.UserDefinedInfo);
                                utilities.modifyLocalFile(config.jobid, config.jobrunid, config.instanceid, config.version, null, consts.FolderFileNameEnum.JobRunCheckpointFile);
                                utilities.modifyLocalFile(config.jobid, config.jobrunid, config.instanceid, config.version, JSON.stringify(config), consts.FolderFileNameEnum.GeneralConfigFile);
                                this.folderReorg(config);
                                if (request.payload.UpdateMode === "Hard") {
                                    ///[TODO]
                                }
                            } else {
                                var errorMessage = "[Read From Blob Error] : " + errors.toString();
                                utilities.log(errorMessage);
                            }
                        }
                    });
                } catch (err) {
                    utilities.log(errorMessage);
                }
            }
        });
    }

    deviceRestartHandler() {
        var filePath = path.join(__dirname, consts.FolderFileNameEnum.JobRunCheckpointFile);
        var runningJobs = {};
        if (fs.existsSync(filePath)) {
            fs.readFile(filePath, 'utf8', (error, data) => {
                try {
                    runningJobs = JSON.parse(data);
                } catch (e) {
                    utilities.log("[Client] Error occur during loading device check point");
                }
                Object.keys(runningJobs).forEach((val) => {
                    utilities.log(`[Client] Get Job run : ${val}`);
                    var version = runningJobs[val]["version"];
                    var jobid = val;
                    var jobrunid = runningJobs[val]["jobRunId"];
                    var instanceid = runningJobs[val]["instanceId"];
                    var readPath = path.join(__dirname, consts.FolderFileNameEnum.EdgeRuntime, version, consts.BlobFileNamesEnum.TstreamsBits);
                    var extractPath = path.join(__dirname, consts.FolderFileNameEnum.NodeModule, consts.FolderFileNameEnum.Runtime);
                    var userDefinedInfoPath = path.join(__dirname, consts.FolderFileNameEnum.Binaries, jobid, jobrunid, instanceid, consts.FolderFileNameEnum.UserDefinedInfo);
                    var jobinfoPath = path.join(__dirname, consts.FolderFileNameEnum.Binaries, jobid, jobrunid, instanceid, consts.FolderFileNameEnum.GeneralConfigFile);
                    if (!fs.existsSync(extractPath)) {
                        mkdirp(extractPath, (err) => {
                            this.extractTstreams(readPath, extractPath, (errorcheck, type, payload) => {
                                if (errorcheck === true) {
                                    fs.readFile(userDefinedInfoPath, (err, userDefinedInfo) => {
                                        if (!err) {
                                            this.sendQueryMessage(jobid, jobrunid, instanceid, version, consts.MethodEnum.StartJobDirectMethodName, userDefinedInfo);
                                        } else {
                                            this.deviceTwinReport(jobid, null, null, consts.DeviceJobStatusEnum.Failed, null, consts.FatalErrorEnum.DeviceRestartFailure);
                                            utilities.modifyLocalFile(jobid, null, null, null, consts.DeviceJobStatusEnum.Failed, consts.FolderFileNameEnum.StatusFile);
                                            utilities.modifyLocalFile(jobid, null, null, null, consts.FatalErrorEnum.DeviceRestartFailure, consts.FolderFileNameEnum.FailureError);
                                            this.deleteLocalFile(jobid, consts.FolderFileNameEnum.JobRunCheckpointFile);
                                        }
                                    });
                                } else {
                                    this.deviceTwinReport(jobid, null, null, consts.DeviceJobStatusEnum.Failed, null, consts.FatalErrorEnum.DeviceRestartFailure);
                                    utilities.modifyLocalFile(jobid, null, null, null, consts.DeviceJobStatusEnum.Failed, consts.FolderFileNameEnum.StatusFile);
                                    utilities.modifyLocalFile(jobid, null, null, null, consts.FatalErrorEnum.DeviceRestartFailure, consts.FolderFileNameEnum.FailureError);
                                    this.deleteLocalFile(jobid, consts.FolderFileNameEnum.JobRunCheckpointFile);
                                }
                            });
                        });
                    } else {
                        this.extractTstreams(readPath, extractPath, (errorcheck, type, payload) => {
                            if (errorcheck === true) {
                                fs.readFile(userDefinedInfoPath, (err, userDefinedInfo) => {
                                    if (!err) {
                                        this.sendQueryMessage(jobid, jobrunid, instanceid, version, consts.MethodEnum.StartJobDirectMethodName, userDefinedInfo);
                                    } else {
                                        this.deviceTwinReport(jobid, null, null, consts.DeviceJobStatusEnum.Failed, null, consts.FatalErrorEnum.DeviceRestartFailure);
                                        utilities.modifyLocalFile(jobid, null, null, null, consts.DeviceJobStatusEnum.Failed, consts.FolderFileNameEnum.StatusFile);
                                        utilities.modifyLocalFile(jobid, null, null, null, consts.FatalErrorEnum.DeviceRestartFailure, consts.FolderFileNameEnum.FailureError);
                                        this.deleteLocalFile(jobid, consts.FolderFileNameEnum.JobRunCheckpointFile);
                                    }
                                });
                            } else {
                                this.deviceTwinReport(jobid, null, null, consts.DeviceJobStatusEnum.Failed, null, consts.FatalErrorEnum.DeviceRestartFailure);
                                utilities.modifyLocalFile(jobid, null, null, null, consts.DeviceJobStatusEnum.Failed, consts.FolderFileNameEnum.StatusFile);
                                utilities.modifyLocalFile(jobid, null, null, null, consts.FatalErrorEnum.DeviceRestartFailure, consts.FolderFileNameEnum.FailureError);
                                this.deleteLocalFile(jobid, consts.FolderFileNameEnum.JobRunCheckpointFile);
                            }
                        });
                    }
                });
            });
        }
    }

    /**
    * @description      Validate request payload.
    *
    * @param  payload   request payload.
    * @param  methodname  name of current device method.
    * @param  callback  callback to validateRequestPayload
    *
    */
    validateRequestPayload(payload, methodname, callback) {
        switch (methodname) {
            case consts.MethodEnum.StartJobDirectMethodName:
            case consts.MethodEnum.UpdateJobDirectMethodName:
                if (typeof payload.jobInfoUrl !== 'undefined' &&
                    typeof payload.runtimeUrl !== 'undefined' &&
                    typeof payload.userDefinedInfoUrl !== 'undefined' &&
                    typeof payload.jobId !== 'undefined' &&
                    payload.jobInfoUrl &&
                    payload.runtimeUrl &&
                    payload.userDefinedInfoUrl &&
                    payload.jobId) {
                    utilities.log("[Client] Start job Request Payload is valid!");
                    callback(false);
                } else {
                    utilities.log("[Client] Start job Request Payload is not valid!");
                    callback(true);
                }
                break;
            case consts.MethodEnum.StopJobDirectMethodName:
                if (typeof payload.jobId !== 'undefined' &&
                    typeof payload.jobRunId !== 'undefined' &&
                    typeof payload.instanceId !== 'undefined' &&
                    payload.jobId &&
                    payload.jobRunId &&
                    payload.instanceId) {
                    utilities.log("[Client] Stop job Request Payload is valid!");
                    callback(false);
                } else {
                    utilities.log("[Client] Stop job Request Payload is not valid!");
                    callback(true);
                }
                break;
            case consts.MethodEnum.UpdateJobDirectMethodName:
                if (typeof payload.jobInfoUrl !== 'undefined' &&
                    typeof payload.runtimeUrl !== 'undefined' &&
                    typeof payload.userDefinedInfoUrl !== 'undefined' &&
                    typeof payload.jobId !== 'undefined' &&
                    typeof payload.UpdateMode !== 'undefined' &&
                    payload.jobInfoUrl &&
                    payload.runtimeUrl &&
                    payload.userDefinedInfoUrl &&
                    payload.jobId &&
                    payload.UpdateMode) {
                    utilities.log("Update job Request Payload is valid!");
                    callback(false);
                } else {
                    utilities.log("Update job Request Payload is not valid!");
                    callback(true);
                }
                break;
        }
    }

    /**
      * @description      Check if number of running on GW exceeds the limit
      *
      * @param  callback  callback to checkJobCountFull
      */
    checkJobCountFull(callback) {
        try {
            var filePath = path.join(__dirname, consts.FolderFileNameEnum.JobRunCheckpointFile);
            fs.lstat(filePath, (err, stats) => {
                if (err) {
                    callback(false);
                } else {
                    fs.readFile(filePath, 'utf8', (error, data) => {
                        var runningJob = JSON.parse(data);
                        if (runningJob.length >= this.jobCount) {
                            callback(true);
                        } else {
                            callback(false);
                        }
                    });
                }
            });
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Check if given jobid and jobrunid exist 
    *
    * @param  payload   payload of request, contains jobid, jobrunid and jobinstanceid
    * @param  callback  callback to checkJobRunExist
    * 
    */
    checkJobRunExist(payload, callback) {
        try {
            var filePath = path.join(__dirname, consts.FolderFileNameEnum.JobRunCheckpointFile);
            fs.lstat(filePath, (err, stats) => {
                if (err) {
                    callback(false);
                } else {
                    fs.readFile(filePath, 'utf8', (error, data) => {
                        var runningJob = JSON.parse(data);
                        var jobId = payload.jobId;
                        var jobRunId = payload.jobRunId;
                        if (runningJob.hasOwnProperty(jobId) && runningJob[jobId]["jobRunId"] == jobRunId) {
                            callback(true);
                        } else {
                            callback(false);
                        }
                    });
                }
            });
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Download Configuration, Tstreambits and query from Blob
    *
    * @param  payload   payload of request, contains three urls.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadFromBlob(payload, callback) {
        try {
            this.downloadConfiguration(payload, callback);
            this.downloadTstreamsBits(payload, callback);
            this.downloadUserDefinedJobInfo(payload, callback);
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Download Configuration, Tstreambits and query from Blob for job upgrade
    *
    * @param  payload   payload of request, contains three urls.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadFromBlobUpgrade(payload, callback) {
        this.downloadConfiguration(payload, callback);
        this.softUpdateTstreamsBits(payload, callback);
        this.downloadUserDefinedJobInfo(payload, callback);
    }

    /**
    * @description      Parse given url and download configuration.txt
    *
    * @param  payload   payload from Start job / Upgrade job DM.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadConfiguration(payload, callback) {
        let url = payload.jobInfoUrl;
        let slash = url.indexOf('blob.core.windows.net/') + 22;
        let secondslash = url.indexOf('/', slash);
        let questionmark = url.indexOf('?', secondslash);
        let hosturl = url.substring(0, slash);
        let container = url.substring(slash, secondslash);
        let blob = url.substring(secondslash + 1, questionmark);
        let asakey = url.substring(questionmark + 1, url.length);
        this.blobSvc = azureStorage.createBlobServiceWithSas(hosturl, asakey);
        //this.blobSvc = azureStorage.createBlobServiceAnonymous(hosturl);
        try {
            this.blobSvc.getBlobToLocalFile(container, blob, path.join(__dirname, consts.BlobFileNamesEnum.JobInfo), (error) => {
                if (!!error) {
                    callback(false, consts.BlobDownloadEnum.Error, error);
                } else {
                    callback(true, consts.BlobDownloadEnum.JobInfo, null);
                }
            });
        }
        catch (err) {
            callback(false, consts.BlobDownloadEnum.Error, err);
        }
    }

    /**
    * @description      Parse given url, download and extract Tstreams Bits.
    *
    * @param  payload   payload from Start job DM.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadTstreamsBits(payload, callback) {
        let url = payload.runtimeUrl;
        let slash = url.indexOf('blob.core.windows.net/') + 22;
        let secondslash = url.indexOf('/', slash);
        let questionmark = url.indexOf('?', secondslash);
        let hosturl = url.substring(0, slash);
        let container = url.substring(slash, secondslash);
        let blob = url.substring(secondslash + 1, questionmark);
        let asakey = url.substring(questionmark + 1, url.length);
        this.blobSvc = azureStorage.createBlobServiceWithSas(hosturl, asakey);
        //this.blobSvc = azureStorage.createBlobServiceAnonymous(hosturl);
        try {
            this.blobSvc.getBlobToLocalFile(container, blob, path.join(__dirname, consts.BlobFileNamesEnum.TstreamsBits), (error) => {
                if (!!error) {
                    callback(false, consts.BlobDownloadEnum.Error, error);
                } else {
                    var readPath = path.join(__dirname, consts.BlobFileNamesEnum.TstreamsBits);
                    var extractPath = path.join(__dirname, consts.FolderFileNameEnum.NodeModule, consts.FolderFileNameEnum.Runtime);
                    if (!fs.existsSync(extractPath)) {
                        mkdirp(extractPath, (err) => {
                            this.extractTstreams(readPath, extractPath, callback);
                        });
                    } else {
                        this.extractTstreams(readPath, extractPath, callback);
                    }
                }
            });
        } catch (err) {
            callback(false, consts.BlobDownloadEnum.Error, err);
        }
    }

    /**
    * @description      Parse given url, download and extract Tstreams Bits.
    *
    * @param  payload   payload from Start job / Upgrade job DM.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    softUpdateTstreamsBits(payload, callback) {
        let url = payload.runtimeUrl;
        let slash = url.indexOf('blob.core.windows.net/') + 22;
        let secondslash = url.indexOf('/', slash);
        let questionmark = url.indexOf('?', secondslash);
        let hosturl = url.substring(0, slash);
        let container = url.substring(slash, secondslash);
        let blob = url.substring(secondslash + 1, questionmark);
        let asakey = url.substring(questionmark + 1, url.length);
        this.blobSvc = azureStorage.createBlobServiceWithSas(hosturl, asakey);
        //this.blobSvc = azureStorage.createBlobServiceAnonymous(hosturl);
        try {
            this.blobSvc.getBlobToLocalFile(container, blob, path.join(__dirname, consts.BlobFileNamesEnum.TstreamsBits), (error) => {
                if (!!error) {
                    callback(false, consts.BlobDownloadEnum.Error, error);
                } else {
                    callback(true, consts.BlobDownloadEnum.TstreamsBits, null)
                }
            });
        } catch (err) {
            callback(false, consts.BlobDownloadEnum.Error, err);
        }
    }

    /**
    * @description      Parse given url and get compiled query.
    *
    * @param  payload   payload from Start job / Upgrade job DM.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadQuery(payload, callback) {
        let url = payload.userDefinedInfoUrl;
        let slash = url.indexOf('blob.core.windows.net/') + 22;
        let secondslash = url.indexOf('/', slash);
        let questionmark = url.indexOf('?', secondslash);
        let hosturl = url.substring(0, slash);
        let container = url.substring(slash, secondslash);
        let blob = url.substring(secondslash + 1, questionmark);
        let asakey = url.substring(questionmark + 1, url.length);
        this.blobSvc = azureStorage.createBlobServiceWithSas(hosturl, asakey);
        //this.blobSvc = azureStorage.createBlobServiceAnonymous(hosturl);
        try {
            this.blobSvc.getBlobToText(container, blob, (error, result, response) => {
                if (!!error) {
                    callback(false, consts.BlobDownloadEnum.Error, error);
                } else {
                    callback(true, consts.BlobDownloadEnum.Query, result);
                }
            });
        } catch (err) {
            callback(false, consts.BlobDownloadEnum.Error, err);
        }
    }

    /**
    * @description      Parse given url and get user defined jobinfo.
    *
    * @param  payload   payload from Start job / Upgrade job DM.
    * @param  callback  callback to downloadFromBlob.
    *
    */
    downloadUserDefinedJobInfo(payload, callback) {
        let url = payload.userDefinedInfoUrl;
        let slash = url.indexOf('blob.core.windows.net/') + 22;
        let secondslash = url.indexOf('/', slash);
        let questionmark = url.indexOf('?', secondslash);
        let hosturl = url.substring(0, slash);
        let container = url.substring(slash, secondslash);
        let blob = url.substring(secondslash + 1, questionmark);
        let asakey = url.substring(questionmark + 1, url.length);
        this.blobSvc = azureStorage.createBlobServiceWithSas(hosturl, asakey);
        //this.blobSvc = azureStorage.createBlobServiceAnonymous(hosturl);
        try {
            this.blobSvc.getBlobToText(container, blob, (error, result, response) => {
                if (!!error) {
                    callback(false, consts.BlobDownloadEnum.Error, error);
                } else {
                    callback(true, consts.BlobDownloadEnum.UserdefinedJobInfo, result);
                }
            });
        } catch (err) {
            callback(false, consts.BlobDownloadEnum.Error, err);
        }
    }

    /**
    * @description      Maintain folder structure.
    *
    * @param  config   configuration received from DM
    *
    */
    folderReorg(config) {
        try {
            var extractPath = path.join(__dirname, consts.FolderFileNameEnum.EdgeRuntime, config.version);
            var readPath = path.join(__dirname, consts.BlobFileNamesEnum.TstreamsBits);
            if (fs.existsSync(readPath)) {
                if (!fs.existsSync(extractPath)) {
                    mkdirp(extractPath, (err) => {
                        fs.createReadStream(readPath).pipe(fs.createWriteStream(path.join(extractPath, consts.BlobFileNamesEnum.TstreamsBits)));
                    });
                }
            }
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description      Extract Tstreams bits 
    *
    */
    extractTstreams(readPath, extractPath, callback) {
        utilities.log("[Extract Tstreams]");
        try {
            var extractor = unzip.Extract({ path: extractPath })
                .on('error', function () { if (callback) { callback(false, "Error", "Extract tstream.jar error") } })
                .on('close', function () { if (callback) { callback(true, consts.BlobDownloadEnum.TstreamsBits, null) } });

            fs.createReadStream(readPath)
                .on('error', function () { if (callback) { callback(false, "Error", "Extract tstream.jar error") } })
                .pipe(extractor);
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description     Print out Twin Status
    *
    */
    printDeviceTwinStatus(twin) {
        utilities.log(`twin Tags : ${JSON.stringify(twin.tags)}`);
        utilities.log(`twin Properties reported: ${JSON.stringify(twin.properties.reported)}`);
        utilities.log(`twin Properties desired: ${JSON.stringify(twin.properties.desired)}`);
    }

    /**
    * @description     Write job info to disk.
    *
    * @param  config   job configuration.
    * @param  query  compiled query.
    * @param  methodname  device method name.
    *
    */
    writeToDisk(config, userdefinedjobinfo, methodname) {
        /// format of config is: 
        /// ["jobid=00000000-0000-0000-0000-000000000000","jobrunid=09709d43-4a64-4d17-ad98-049190fc5f5b","instanceid=01b30c62-83f6-4721-93d6-55bb2659f995","subscriptionid=88980f62-8521-468c-ab8a-7e6b4e8a2573","resourcegroup=250bf595-7090-446e-9b5d-0c8bfc4cc654","resourcename=ac2808db-3b0d-4b7f-aeb1-5c8ce301cd3e","inputs=name-type,name-type"]  
        var jobid = config.jobid;
        var jobrunid = config.jobrunid;
        var instanceid = config.instanceid;
        var version = config.version;
        utilities.modifyLocalFile(jobid, jobrunid, instanceid, version, JSON.stringify(config), consts.FolderFileNameEnum.GeneralConfigFile);
        utilities.modifyLocalFile(jobid, jobrunid, instanceid, version, userdefinedjobinfo, consts.FolderFileNameEnum.UserDefinedInfo);
        utilities.modifyLocalFile(jobid, jobrunid, instanceid, version, methodname, consts.FolderFileNameEnum.LastUserAction);
        utilities.modifyLocalFile(jobid, jobrunid, instanceid, version, null, consts.FolderFileNameEnum.JobRunCheckpointFile);
    }

    /**
    * @description     Delete existing local files.
    *
    * @param  jobid     jobid.
    * @param  filename  name of the file to update.
    *
    */
    deleteLocalFile(jobid, filename) {
        try {
            switch (filename) {
                case consts.FolderFileNameEnum.JobRunCheckpointFile:
                    var filePath = path.join(__dirname, filename);
                    var runningJobs = {};
                    if (fs.existsSync(filePath)) {
                        fs.readFile(filePath, 'utf8', (error, data) => {
                            runningJobs = JSON.parse(data);
                            delete runningJobs[jobid];
                            fs.writeFile(filePath, JSON.stringify(runningJobs), (err) => {
                                if (!!err) {
                                    utilities.log(`Update local ${filename} failed`);
                                }
                            });
                        });
                    }
                    break;
            }
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description     Send content to ASA module.
    *
    * @param  jobid   job id.
    * @param  methodname  device method name.
    * @param  content  message content, which is compiled query when start a job, empty when stop a job.
    *
    */
    sendQueryMessage(jobid, jobrunid, instanceid, version, methodname, userdefinedjobinfo) {
        try {
            this.messageBus.publish({
                properties: {
                    'source': 'client',
                    'type': methodname,
                    'jobid': jobid,
                    'jobrunid': jobrunid,
                    'instanceid': instanceid,
                    'version': version
                },
                content: new Uint8Array(Buffer.from(userdefinedjobinfo))
            });
        } catch (err) {
            utilities.log(err.toString());
        }
    }

    /**
    * @description     Destroy module.
    *
    */
    destroy() {
        utilities.log('client.destroy');
        if (this.connected) {
            this.iothub_client.close((err) => {
                if (err) {
                    utilities.log(`[Client] An error occurred when disconnecting from Azure IoT Hub: ${err.toString()}`);
                }
            });
        }
    }
}
module.exports = new iothubdt();
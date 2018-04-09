'use strict';

const fs = require('fs');
const path = require('path');
const mkdirp = require('mkdirp');
const consts = require('./constants.js');
const Utilities = require('./utilities.js');

var utilities = null;
var TStreamsMain = null;

module.exports = {
    messageBus: null,
    configuration: null,
    subscriptionId: null,
    resourceGroup: null,
    resourceName: null,
    jobid: null,
    jobrunid: null,
    instanceid: null,
    version: null,
    diagnostic: " ",
    status: null,
    userAction: null,
    eventcount: 0,
    output: null,
    query: null,
    querypath: null,
    data: null,
    jobInfo: null,
    jobRunning: null,
    metrics_InputEvents: 0,
    metrics_OutputEvents: 0,
    metrics_Errors: 0,
    MetricsConfigurationSetup: null,

    /**
    * @description      Create function.
    *
    * @param  messageBus   gateway messagebus initilization.
    * @param  configuration  gateway configuration initilization.
    *
    * @return boolean   Indicte whether module initilize properly.
    */
    create: function (messageBus, configuration) {
        this.messageBus = messageBus;
        this.configuration = configuration;
        this.deviceId = this.configuration.deviceid;
        this.procesedEventTag = this.configuration.output_message_name;
        utilities = new Utilities(this.messageBus);

        /// status report
        /// when user action is StartJobDirectMethodName
        /// if diagnostic path is not empty, reported degraded
        /// else if eventcount equals to 0, reported idle
        /// else if eventcount NOT equals to 0, reported PROCESSING
        /// if user action is StopJobDirectMethodName report current status.
        /// when receive new user action, reported initializing or stopped accordingly
        /// report fail status, when having fatal errors      
        setInterval(() => {
            if (this.jobid) {
                var diagnosticPath = path.join(__dirname, consts.FolderFileNameEnum.Data, this.jobid, consts.FolderFileNameEnum.DiagnosticFile);
                if (this.userAction === consts.MethodEnum.StartJobDirectMethodName) {
                    if (this.status === consts.DeviceJobStatusEnum.Failed) {
                        this.sendStatusMessage();
                        this.query = consts.emptyquery;
                        this.jobInfo = consts.emptyquery;
                        this.addQuery(this.query);
                        this.deleteLocalFile(this.jobid, consts.FolderFileNameEnum.JobRunCheckpointFile);
                        this.jobRunning = false;
                    } else if (this.jobid && this.jobrunid && this.instanceid) {
                        fs.lstat(diagnosticPath, (err, stats) => {
                            if (!!err) {
                                if (this.eventcount === 0) {
                                    this.status = consts.DeviceJobStatusEnum.Idle;
                                } else {
                                    this.status = consts.DeviceJobStatusEnum.Processing;
                                }
                                this.sendStatusMessage();
                                utilities.modifyLocalFile(this.jobid, this.jobrunid, this.instanceid, this.version, this.status, consts.FolderFileNameEnum.StatusFile);
                                this.diagnostic = " ";
                                this.eventcount = 0;
                            } else {
                                this.status = consts.DeviceJobStatusEnum.Degraded;
                                this.diagnostic = fs.readFileSync(diagnosticPath, 'utf8');
                                this.sendStatusMessage();
                                utilities.modifyLocalFile(this.jobid, this.jobrunid, this.instanceid, this.version, this.status, consts.FolderFileNameEnum.StatusFile);
                                this.diagnostic = " ";
                                this.eventcount = 0;
                                fs.unlinkSync(diagnosticPath);
                            }
                        });
                    } else {
                        this.sendStatusMessage();
                        utilities.modifyLocalFile(this.jobid, this.jobrunid, this.instanceid, this.version, this.status, consts.FolderFileNameEnum.StatusFile);
                    }
                } else if (this.userAction === consts.MethodEnum.StopJobDirectMethodName) {
                    this.sendStatusMessage();
                    utilities.modifyLocalFile(this.jobid, this.jobrunid, this.instanceid, this.version, this.status, consts.FolderFileNameEnum.StatusFile);
                    this.eventcount = 0;
                    fs.lstat(diagnosticPath, (err, stats) => {
                        if (!err) {
                            fs.unlinkSync(diagnosticPath);
                        }
                    });
                    this.jobRunning = false;
                } else {
                    fs.lstat(diagnosticPath, (err, stats) => {
                        if (!err) {
                            this.status = consts.DeviceJobStatusEnum.Degraded;
                            this.diagnostic = fs.readFileSync(diagnosticPath, 'utf8');
                            this.sendStatusMessage();
                            this.diagnostic = " ";
                            this.eventcount = 0;
                            fs.unlinkSync(diagnosticPath);
                        }
                    });
                }
            }
        }, consts.statusReportFrequency);

        setInterval(() => {
            if (this.jobRunning === false) {
                this.cleanUpJobInfo();
            }
        }, consts.jobInfoCleanUpCheckFrequency)

        /// send metrics to message broker every 1 min
        /// metrics are aggreated value with 1 min window
        /// current available metrics are 'InputEvents' & 'OutputEvents'        
        setInterval(() => {
            if (this.query != consts.emptyquery || this.metrics_InputEvents || this.metrics_OutputEvents) {
                if (!this.MetricsConfigurationSetup) {
                    this.MetricsInit();
                }

                if (!this.MetricsConfigurationSetup) {
                    utilities.log("[Metric] Fail to init metrics logging");
                    return;
                }

                var metricstime = new Date();
                let json = JSON.stringify(
                    {
                        'SubscriptionId': this.subscriptionId,
                        'ResourceGroupName': this.resourceGroup,
                        'ResourceName': this.resourceName,
                        'Metrics': [{
                            'Recordtime': metricstime,
                            'Metrictype': 'inputevents',
                            'Value': this.metrics_InputEvents
                        },
                            {
                                'Recordtime': metricstime,
                                'Metrictype': 'outputevents',
                                'Value': this.metrics_OutputEvents
                            }]
                    });
                this.messageBus.publish({
                    properties: {
                        'source': 'asamodule',
                        'type': 'ASAMetrics',
                        'deviceId': this.deviceId,
                        'subscriptionId': this.subscriptionId,
                        'resourceGroup': this.resourceGroup,
                        'resourceName': this.resourceName,
                        'jobId': this.jobid,
                        'jobRunId': this.jobrunid,
                        'instanceId': this.instanceid,
                        'recordtime': metricstime
                    },
                    content: new Uint8Array(Buffer.from(json))
                });
                utilities.log("[Metric] send one metrcis to message broker");
                this.metrics_InputEvents = 0;
                this.metrics_OutputEvents = 0;
            }
        }, consts.metricslogintervalinseconds);
        return true;
    },

    /**
    * @description     Receive message from iot module and sensor module.
    *
    * @param  message   message from messgae bus.
    *
    */
    receive: function (message) {
        let buf = Buffer.from(message.content);
        let receive = buf.toString();
        if (message.properties.source === 'client') {
            if (message.properties.type === consts.MethodEnum.StartJobDirectMethodName) {
                utilities.log(`[ASA] receives start request ${message.properties.jobid}`);
                this.status = consts.DeviceJobStatusEnum.Initializing;
                this.userAction = consts.MethodEnum.StartJobDirectMethodName;
                this.jobRunning = true;
                this.jobInfo = receive;
                this.jobid = message.properties.jobid;
                this.jobrunid = message.properties.jobrunid;
                this.instanceid = message.properties.instanceid;
                this.version = message.properties.version;
                this.parseJobInfo(this.jobInfo);
                this.MetricsInit();
            } else if (message.properties.type === consts.MethodEnum.StopJobDirectMethodName) {
                utilities.log(`[ASA] receives stop request ${message.properties.jobid}`);
                /// [TODO] Log error if jobid and runid not fit current device
                if (message.properties.jobid === this.jobid &&
                    message.properties.jobrunid === this.jobrunid &&
                    message.properties.instanceid === this.instanceid) {
                    this.query = consts.emptyquery;
                    this.userAction = consts.MethodEnum.StopJobDirectMethodName;
                    this.status = consts.DeviceJobStatusEnum.Stopped;
                    this.query = consts.emptyquery;
                    this.jobInfo = consts.emptyquery;
                    this.addQuery(this.query);
                }
            } else {
                this.jobid = message.properties.jobid;
            }
        } else if (message.properties.source === 'sensor') {
            this.data = receive;
            if (this.query) {
                this.addData(this.data);
            }
        }
    },

    /**
    * @description     Send Job status to Iot module.
    *
    */
    sendStatusMessage: function () {
        this.messageBus.publish({
            properties: {
                'type': 'Status',
                'jobid': this.jobid,
                'jobrunid': this.jobrunid,
                'instanceid': this.instanceid,
                'version': this.version,
                'status': this.status
            },
            content: new Uint8Array(Buffer.from(this.diagnostic))
        });
    },

    /**
    * @description    Parse User Defined Job Info, add query, function, reference data to Tstreams
    *
    * @param  content   Includes compiled query, udfFunction, input info, reorder policy. it will be empty when user action is stop.
    *
    */
    parseJobInfo: function (content) {
        var jsonContent = JSON.parse(content);
        this.addQuery(jsonContent[consts.EdgeLogicalJobEnum.Script]);
        this.addFunctions(jsonContent[consts.EdgeLogicalJobEnum.Functions]);
        this.addReorderPolicy(jsonContent[consts.EdgeLogicalJobEnum.EventsOutOfOrderPolicy], jsonContent[consts.EdgeLogicalJobEnum.EventsOutOfOrderMaxDelayInMs]);
        this.addReferenceData(jsonContent[consts.EdgeLogicalJobEnum.Inputs]);
        this.query = jsonContent[consts.EdgeLogicalJobEnum.Script];
    },

    /**
    * @description     Add query into Tstreams.
    *
    * @param  q   compiled query, it will be empty when user action is stop.
    *
    */
    addQuery: function (q) {
        if (typeof q !== 'undefined' && q) {
            let serializedQuery = JSON.parse(q);
            TStreamsMain = this.requireUncached('TStreamsMain');
            // Start TStreams query
            TStreamsMain.startTStreamsQuery(serializedQuery);
        } else {
            TStreamsMain = this.requireUncached('TStreamsMain');
        }
    },

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
    },

    /**
    * @description     Add functions into Tstreams.
    *
    * @param  function  Udf functions.
    *
    */
    addFunctions: function (functions) {
        if (TStreamsMain != null) {
            functions.forEach((element) => {
                var code = {};
                var udfScript = "(" + element.Script + ")";
                try {
                    var funcObject = eval(udfScript);
                } catch (e) {
                    var errorMessage = '[ASA] Parse UDF function from script failed, ASA failed to initialize - ' + e.stack;
                    utilities.log(errorMessage);
                    this.status = consts.DeviceJobStatusEnum.Failed;
                    this.diagnostic = consts.FatalErrorEnum.InvalidUDFFunction;
                }
                if (typeof funcObject === "undefined") {
                    utilities.log(consts.FatalErrorEnum.InvalidUDFFunction);
                    this.status = consts.DeviceJobStatusEnum.Failed;
                    this.diagnostic = consts.FatalErrorEnum.InvalidUDFFunction;
                }
                code["code"] = funcObject;

                var registeredFunctions = {};
                registeredFunctions[element.Name] = code;
                TStreamsMain.addUDFfunction(registeredFunctions);
                utilities.log("[ASA] Added UDFfunction")
            });
        }
    },

    /**
    * @description     Add Reorder policy and latency into Tstreams.
    *
    * @param  policy  reorder policy.
    * @param  latency  reorder latency.
    *
    */
    addReorderPolicy: function (policy, latency) {
        try {
            if (latency) {
                utilities.log(`[ASA] Get reorder latency - ${latency}`);
                TStreamsMain.addReorderLatency(latency);
                utilities.log(`[ASA] Get reorder policy - ${policy}`);
                if (policy === 0) {
                    TStreamsMain.addReorderPolicy("Adjust");
                } else if (policy === 1) {
                    TStreamsMain.addReorderPolicy("Drop");
                }
            }
        } catch (err) {
            utilities.log(errorMessage);
            this.status = consts.DeviceJobStatusEnum.Failed;
            this.diagnostic = consts.FatalErrorEnum.InvalidReorderConfiguration;
        }
    },

    addReferenceData: function (referenceData) {
        if (TStreamsMain != null) {
            referenceData.forEach((element) => {
                if (element.RefDataLocalPath != null) {
                    try {
                        TStreamsMain.addInputs(JSON.parse(fs.readFileSync(element.RefDataLocalPath)));
                        utilities.log("[ASA] Added reference data")
                    } catch (e) {
                        var errorMessage = '[ASA] Got error when loading reference, ASA failed to initialize - ' + e.stack;
                        utilities.log(errorMessage);
                        this.status = consts.DeviceJobStatusEnum.Failed;
                        this.diagnostic = consts.FatalErrorEnum.InvalidReferenceData;
                    }
                }
            });
        }
    },

    /**
    * @description     Add data event into Tstreams.
    *
    * @param  d   data event.
    *
    */
    addData: function (d) {
        this.eventcount++;
        var jsonObject = JSON.parse(d);
        if (this.query != consts.emptyquery) {
            this.metrics_InputEvents += this.getInputEventCount(jsonObject);
        }
        if (TStreamsMain != null) {
            this.output = TStreamsMain.addInputs(jsonObject);
            var validOutput = false;
            Object.keys(this.output.results).forEach((key) => {
                if (this.output.results[key] !== undefined && this.output.results[key].length > 0) {
                    validOutput = true;
                }
            });
            if (validOutput === true) {
                this.metrics_OutputEvents += this.getOutputEventCount(this.output);
                this.messageBus.publish({
                    properties: {
                        'source': 'ASAmodule',
                        'name': this.procesedEventTag
                    },
                    content: new Uint8Array(Buffer.from(JSON.stringify(this.output)))
                });
            }
        }
    },

    /**
    * @description     reload Tstreams modules.
    *
    * @param  module   module name.
    *
    */
    requireUncached: function (module) {
        delete require.cache[require.resolve(module)];
        return require(module);
    },

    getInputEventCount: function (inputs) {
        var eventCount = 0;
        inputs.forEach(function (input) {
            eventCount += input.content.length
        });
        return eventCount;
    },

    getOutputEventCount: function (output) {
        var eventCount = 0;
        eventCount = output.results[Object.keys(this.output.results)[0]].length;
        return eventCount;
    },

    /**
    * @description     Initialize Metrics logging.
    *
    */
    MetricsInit: function () {
        if (!(this.jobid && this.jobrunid && this.instanceid)) {
            this.MetricsConfigurationSetup = false;
            utilities.log("[Metric] Need jobid, jobrunid and instanceid to start metrics logging, one or more is missing");
            return;
        }
        var configurationFilePath = path.join(__dirname, consts.FolderFileNameEnum.Binaries, this.jobid, this.jobrunid, this.instanceid, consts.FolderFileNameEnum.GeneralConfigFile);
        if (fs.existsSync(configurationFilePath)) {
            try {
                var data = fs.readFileSync(configurationFilePath, 'utf8');
                var jsonconfig = JSON.parse(data);
                if (jsonconfig.subscriptionid && jsonconfig.resourcegroup && jsonconfig.resourcename) {
                    this.subscriptionId = jsonconfig.subscriptionid;
                    this.resourceGroup = jsonconfig.resourcegroup;
                    this.resourceName = jsonconfig.resourcename;
                    this.MetricsConfigurationSetup = true;
                }
            }
            catch (err) {
                utilities.log(`[Metric] Fail to read configuration file for job ${this.jobid} with error: ${err}`);
                this.MetricsConfigurationSetup = false;
            }
        } else {
            utilities.log("[Metric] Configuration file doest not exist..., can not initialize metrics");
            this.MetricsConfigurationSetup = false;
        }
    },

    /**
    * @description     Clean up jobinfo from memory when job stopped/failed/deleted.
    *
    */
    cleanUpJobInfo: function () {
        this.jobid = null;
        this.jobrunid = null;
        this.instanceid = null;
        this.version = null;
        this.status = null;
        this.diagnostic = " ";
    },

    /**
    * @description     Destroy module.
    *
    */
    destroy: function () {
        utilities.log('asa.destroy');
    }
};
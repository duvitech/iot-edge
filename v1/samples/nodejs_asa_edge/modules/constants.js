'use strict';

module.exports = {
    MethodEnum: {
        StartJobDirectMethodName: "StartEdgeStreamingJob",
        StopJobDirectMethodName: "StopEdgeStreamingJob",
        UpdateJobDirectMethodName: "UpdateStreamingJob",
        PingDirectMethodName: "PingDevice",
        ShutDownDeviceDirectMethodName: "ShutDownDevice"
    },

    FatalErrorEnum: {
        InvalidRequestPayloadFormat: "Error! Invalid payload format!",
        NoSuchJobRunningOnDevice: "No Such Job Running On Device, could not find this jobid or jobrunid",
        FailedDownloadingFromBlob: "Failed Downloading From Blob",
        JobConflictOnDevice: "No available ASA module for job processing, there is one job currently running on device",
        InvalidUDFFunction: "Parse UDF function from script failed  - Invalid user defined function",
        InvalidReferenceData: "Parse ReferenceData from user defined file failed  - Invalid reference data path",
        InvalidReorderConfiguration: "Failed to add reorder policy and latency to Tstreams",
        DeviceRestartFailure: "Job unable to restart, failed to get job info"
    },

    BlobDownloadEnum: {
        JobInfo: "Configuration",
        TstreamsBits: "Tstreams",
        Query: "Query",
        UserdefinedJobInfo: "UserdefinedJobInfo",
        Error: "Error"
    },

    BlobFileNamesEnum: {
        JobInfo: "jobinfo.json",
        TstreamsBits: "runtime.jar",
        Query: "compiledquery.txt",
        UserDefinedInfo: "UserDefinedInfo.txt"
    },

    DeviceJobStatusEnum: {
        Initializing: "Initializing",
        Idle: "Idle",
        Processing: "Processing",
        Degraded: "Degraded",
        Stopped: "Stopped",
        Failed: "Failed"
    },

    FolderFileNameEnum: {
        Binaries: "Binaries",
        Data: "Data",
        Logs: "Logs",
        NodeModule: "node_modules",
        Runtime: "TStreamsMain",
        EdgeRuntime: "EdgeRuntime",
        QueryFile: "CompiledQuery.txt",
        UserDefinedInfo: "UserDefinedInfo.txt",
        LastUserAction: "LastUserAction.txt",
        StatusFile: "Status.txt",
        GeneralConfigFile: "Configuration.txt",
        DiagnosticFile: "Diagnostic.txt",
        FailureError: "FailureError.txt",
        LogFile: "logFile.txt",
        JobRunCheckpointFile: "JobRunCheckpoint.txt"
    },

    EdgeLogicalJobEnum: {
        Script: "Script",
        Functions: "Functions",
        Inputs: "Inputs",
        Outputs: "Outputs",
        EventsOutOfOrderPolicy: "EventsOutOfOrderPolicy",
        EventsOutOfOrderMaxDelayInMs: "EventsOutOfOrderMaxDelayInMs"
    },

    MessageBusPropertyEnum: {
        Log: "log"
    },

    emptycontent: " ",
    emptyquery: null,
    deviceTwinEdgeSessionName: "streamingedge",
    metricsDiscardLatency: 20 * 60 * 1000,
    clientConnectionCheckFrequency: 60 * 1000,
    statusReportFrequency: 10 * 1000,
    jobInfoCleanUpCheckFrequency: 30 * 1000,
    metricslogintervalinseconds: 60 * 1000,
    quota: 128 * 1024 * 1024,  /// 128 MB
    quotapercent: 0.8,
    gccheckfrequency: 60 * 60 * 1000, /// 1 h
    blobLimitInBytes: 4 * 1024 * 1024 /// 4 MB
};
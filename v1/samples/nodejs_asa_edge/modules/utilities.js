'use strict';

const consts = require('./constants.js');
const fs = require('fs');
const mkdirp = require('mkdirp');
const path = require('path');

function Utility(messageBus) {
    this.messageBus = messageBus;
}

/**
* @description        Parse time format to '_yyyy_MM_dd_hh_mm'
*
* @param  {string}    date   date
*
* @return {float}     _yyyy_MM_dd_hh_mm
*/
Utility.prototype.timeparser = function timeparser(date) {
    try {
        var formatted = '_yyyy_MM_dd_hh_mm'
            .replace(/dd/g, this.addZero(date.getUTCDate()))
            .replace(/MM/g, this.addZero(date.getUTCMonth() + 1))
            .replace(/y{1,4}/g, date.getUTCFullYear())
            .replace(/hh/g, this.addZero(date.getUTCHours()))
            .replace(/mm/g, this.addZero(date.getUTCMinutes()))
        return formatted;
    } catch (err) {
        this.log(err.toString());
    }
};

/**
* @description        Pad with zeros
*
* @param  {string}    num   number
* @param  {string}    width    number of digits in number
*
* @return {string}    number pad with zeros
*/
Utility.prototype.padWithZeros = function padWithZeros(num, width) {
    var numAsString = num + "";
    while (numAsString.length < width) {
        numAsString = "0" + numAsString;
    }
    return numAsString;
};

/**
* @description        Pad with zeros
*
* @param  {string}    num   number
*
* @return {string}   number pad with zeros
*/
Utility.prototype.addZero = function addZero(num) {
    return this.padWithZeros(num, 2);
};

/**
* @description        custom array compare functon for metrics/logs filename sort
*
* @param  {string}    a   name of file1
* @param  {string}    b   name of file2
*
* @return {float}     Netative, zero, positive value
*/
Utility.prototype.filenameCompareFunction = function filenameCompareFunction(a, b) {
    try {
        var sectionSeparatorIndex = a.lastIndexOf('_');
        var a_DatePart = a.substr(0, sectionSeparatorIndex);
        var b_DatePart = b.substr(0, sectionSeparatorIndex);
        var a_sectionPart = a.substr(sectionSeparatorIndex + 1, a.length - sectionSeparatorIndex - 1).split('.')[0];
        var b_sectionPart = b.substr(sectionSeparatorIndex + 1, b.length - sectionSeparatorIndex - 1).split('.')[0];
        if (a_DatePart != b_DatePart) {
            return a_DatePart > b_DatePart ? 1 : -1;
        }
        else if (a_sectionPart != b_sectionPart) {
            return parseInt(a_sectionPart) > parseInt(b_sectionPart) ? 1 : -1;
        }
        return 0;
    } catch (err) {
        this.log(err.toString());
    }
};

/**
* @description        print logs out and send logs to logger 
*
* @param  {string}    str   log string
* @param  {string}    messageBus   messageBus
*
* @return {string}    
*/
Utility.prototype.log = function log(str) {
    console.log(str);
    this.messageBus.publish({
        properties: {
            'name': 'log'
        },
        content: new Uint8Array(Buffer.from(str))
    });
};

/**
* @description     Update existing local files.
*
* @param  jobid     jobid.
* @param  jobrunid  jobrunid.
* @param  instanceid  jobinstanceid.
* @param  version   Tstreams version.
* @param  content   content to update.
* @param  filename  name of the file to update.
*
*/
Utility.prototype.modifyLocalFile = function modifyLocalFile(jobid, jobrunid, instanceid, version, content, filename) {
    try {
        switch (filename) {
            case consts.FolderFileNameEnum.GeneralConfigFile:
            case consts.FolderFileNameEnum.QueryFile:
            case consts.FolderFileNameEnum.UserDefinedInfo:
                var folderPath = path.join(__dirname, consts.FolderFileNameEnum.Binaries, jobid, jobrunid, instanceid);
                var filePath = path.join(__dirname, consts.FolderFileNameEnum.Binaries, jobid, jobrunid, instanceid, filename);
                if (!fs.existsSync(folderPath)) {
                    mkdirp(folderPath, (err) => {
                        if (!!err) {
                            this.log(err.toString());
                        } else {
                            fs.writeFile(filePath, content, (err) => {
                                if (!!err) {
                                    this.log(`Update local ${filename} failed`);
                                }
                            });
                        }
                    });
                } else {
                    fs.writeFile(filePath, content, (err) => {
                        if (!!err) {
                            this.log(`Update local ${filename} failed`);
                        }
                    });
                }
                break;
            case consts.FolderFileNameEnum.StatusFile:
            case consts.FolderFileNameEnum.LastUserAction:
            case consts.FolderFileNameEnum.DiagnosticFile:
            case consts.FolderFileNameEnum.FailureError:
                var folderPath = path.join(__dirname, consts.FolderFileNameEnum.Data, jobid);
                var filePath = path.join(__dirname, consts.FolderFileNameEnum.Data, jobid, filename);
                if (!fs.existsSync(folderPath)) {
                    mkdirp(folderPath, (err) => {
                        if (!!err) {
                            this.log(err.toString());
                        } else {
                            fs.writeFile(filePath, content, (err) => {
                                if (!!err) {
                                    this.log(`Update local ${filename} failed`);
                                }
                            });
                        }
                    });
                } else {
                    fs.writeFile(filePath, content, (err) => {
                        if (!!err) {
                            this.log(`Update local ${filename} failed`);
                        }
                    });
                }
                break;
            case consts.FolderFileNameEnum.JobRunCheckpointFile:
                var filePath = path.join(__dirname, filename);
                var runningJobs = {};
                var jobinfo = {};
                jobinfo["jobRunId"] = jobrunid;
                jobinfo["instanceId"] = instanceid;
                jobinfo["version"] = version;
                if (!fs.existsSync(filePath)) {
                    runningJobs[jobid] = jobinfo;
                    fs.writeFile(filePath, JSON.stringify(runningJobs), (err) => {
                        if (!!err) {
                            this.log(`Update local ${filename} failed`);
                        }
                    });
                } else {
                    fs.readFile(filePath, 'utf8', (error, data) => {
                        runningJobs = JSON.parse(data);
                        runningJobs[jobid] = jobinfo;
                        fs.writeFile(filePath, JSON.stringify(runningJobs), (err) => {
                            if (!!err) {
                                this.log(`Update local ${filename} failed`);
                            }
                        });
                    });
                }
                break;
            default:
                var folderPath = path.join(__dirname, consts.FolderFileNameEnum.Logs);
                var filePath = path.join(__dirname, consts.FolderFileNameEnum.Logs, filename);
                if (!fs.existsSync(folderPath)) {
                    mkdirp(folderPath, (err) => {
                        fs.appendFile(filePath, content, (err) => {
                            if (err) {
                                this.log(err.toString());
                            }
                        });
                    });
                } else {
                    fs.appendFile(filePath, content, (err) => {
                        if (err) {
                            this.log(err.toString());
                        }
                    });
                }
                break;
        }
    } catch (err) {
        this.log(err.toString());
    }
};

module.exports = Utility;
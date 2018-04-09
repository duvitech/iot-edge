'use strict';

const consts = require('./constants.js');
const fs = require('fs');
const getSize = require('get-folder-size');
const Path = require('path');
const Utilities = require('./utilities.js');

var utilities = null;

module.exports = {
    messageBus: null,
    configuration: null,
    loggercheckpoint: null,
    loggercombinelist: null,
    filenum: null,
    logpath: null,


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
        this.logpath = Path.join(__dirname, consts.FolderFileNameEnum.Logs);
        this.loggercombinelist = {};
        this.filenum = 0;
        utilities = new Utilities(this.messageBus);

        setInterval(() => {
            getSize(this.logpath, (err, usage) => {
                if (!err) {
                    var diskusage = usage / consts.quota;
                    if (diskusage > consts.quotapercent) {
                        fs.readdir(this.logpath, (err, files) => {
                            try {
                                files.sort(utilities.filenameCompareFunction);
                                this.deletefiles(files, usage);
                            } catch (err) {
                                utilities.log(err.toString());
                            }
                        });
                    }
                }
            });
        }, consts.gccheckfrequency);
        return true;
    },

    /**
    * @description     Receive message from iot module and sensor module.
    *
    * @param  message   message from messgae bus.
    *
    */
    receive: function (message) {
        var buf = Buffer.from(message.content);
        var content = buf.toString();
        if (message.properties.name === consts.MessageBusPropertyEnum.Log) {
            var messagetime = utilities.timeparser(new Date());
            var contentsize = this.getBytes(content);
            this.filesplitandlog(messagetime, contentsize, content);
            this.loggercheckpoint = messagetime;
        }
    },

    filesplitandlog: function (messagetime, contentsize, content) {
        try {
            if (this.loggercheckpoint === null || this.loggercheckpoint != messagetime) {
                this.filenum = 0;
                if (contentsize > consts.blobLimitInBytes) {
                    console.log("Message size too large : " + contentsize);
                } else {
                    this.addtofile(content, messagetime, this.filenum);
                }
                this.loggercombinelist[messagetime] = contentsize;
                if (this.loggercheckpoint !== null) {
                    delete this.loggercombinelist[this.loggercheckpoint];
                }
            } else if (this.loggercheckpoint == messagetime) {
                var newsize = Number(this.loggercombinelist[messagetime]) + Number(contentsize);
                if (newsize > consts.blobLimitInBytes) {
                    this.filenum++;
                    this.addtofile(content, messagetime, this.filenum);
                    this.loggercombinelist[messagetime] = contentsize;
                } else {
                    this.addtofile(content, messagetime, this.filenum);
                    this.loggercombinelist[messagetime] += Number(contentsize);
                }
            }
        } catch (err) {
            utilities.log(err.toString());
        };
    },

    deletefiles: function (files, usage) {
        try {
            for (var i = 0; i < files.length; i++) {
                var path = Path.join(this.logpath, files[i]);
                fs.stat(path, function (err, stats) {
                    if (!!err) {
                        return;
                    }
                    if (stats.isFile()) {
                        if ((Number(usage) / Number(this.quota)) < Number(this.quotapercent)) {
                            return;
                        } else {
                            usage = Number(usage) - Number(stats.size);
                            try {
                                fs.unlinkSync(this.path);
                            } catch (err) {
                                utilities.log(err.toString());
                            };
                        }
                    }
                }.bind({ path: path, quotapercent: consts.quotapercent, quota: consts.quota }));
            }
        } catch (err) {
            utilities.log(err.toString());
        }
    },

    getBytes: function (string) {
        return Buffer.byteLength(string, 'utf8');
    },

    addtofile: function (content, messagetime, filenum) {
        var filename = consts.MessageBusPropertyEnum.Log + messagetime + "_" + filenum;
        utilities.modifyLocalFile(null, null, null, null, content, filename);
    },

    destroy: function () {
        console.log('Logger.destroy');
    }
};

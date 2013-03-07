/**
 * 阿里云开放存储服务访问模块
 */

var fs = require("fs"),
	ndir = require("ndir"),
	util = require("util"),
	path = require("path"),
	async = require("async"),
	xml2js = require("xml2js"),
	crypto = require("crypto"),
	mimetypes = require("mime"),
	request = require("request"),
	data2xml = require("data2xml"),
	oppressor = require("oppressor");

var check = require("./check.js"),
	myUtil = require("./util.js"),
	config = require("./config.js");

var oss = function(options) {
	this._accessId = options.accessId;
	this._accessKey = options.accessKey;
	this._host = options.host || "oss.aliyuncs.com";
	this._port = options.port || "80";
	this._timeout = 50000;

	if (this._accessId == "" || this._accessKey == "") {
		throw new Error("OSS accessId and accessKey are both required");
	}
}

module.exports = oss;

var pro = oss.prototype;

/**
 * oss authRequest
 * 
 * @param {Object} options (Optional)
 * @return {Function} cb
 * @api private
 */
pro.authRequest = function(options, cb) {
	var self = this;

	if (!((("/" == options[config.OSS_OBJECT]) || ("" == options[config.OSS_BUCKET])) && ("GET" == options[config.OSS_METHOD])) && !myUtil.validateBucket(options[config.OSS_BUCKET])) {
		throw new Error("oss_bucket : " + options[config.OSS_BUCKET] + " invalid");
	}

	if (myUtil.is_gb2312(options[config.OSS_OBJECT])) {
		throw new Error("current version do not support gb2312 or gbk object name !!!");
	}

	if (options[config.OSS_OBJECT] !== "" && typeof options[config.OSS_OBJECT] === "string" && !myUtil.validateObject(options[config.OSS_OBJECT])) {
		throw new Error("oss_object : " + options[config.OSS_OBJECT] + " invalid");
	}

	if (typeof options[config.OSS_HEADERS] !== "undefined" && typeof options[config.OSS_HEADERS][config.OSS_ACL] === "string" && options[config.OSS_HEADERS][config.OSS_ACL] != "") {
		for (var acl in options[config.OSS_HEADERS][config.OSS_ACL]) {
			var flag = 0, temp = acl.toLowerCase();
			for (var i in options[config.OSS_HEADERS][config.OSS_ACL]) {
				if (temp == i) {
					flag = 1;
					break;
				}
			}
			if (flag == 0) {
				throw new Error("oss_acl : " + options[config.OSS_HEADERS][config.OSS_ACL] + "invalid");
			}
		}
	}

	options["url"] = this.makeURL(options);

	async.waterfall([
		function getHeader(callback) {
			self.getHeaders(options, function(headers) {
				callback(null, headers);
			});
		}, function doRequest(headers, callback) {
			options["headers"] = headers;
			options["timeout"] = options["timeout"] || self._timeout;
			var req = request(options, function(err, response, body) {
				if (err && cb) {
					cb(err);
				}
				if (response && response.statusCode != 200 && response.statusCode != 204) {
					var e = new Error(body);
					e.code = response.statusCode;
					if (cb) {
						cb(e);
					}
				} else {
					if (body && !options.dstFile) {
						var parser = new xml2js.Parser();
						parser.parseString(body, function(error, result) {
							if (error) {
								throw error;
							}
							if (cb) {
								cb(error, result);
							}
						});
					} else {
						if (response && response.headers && cb) {
							cb(err, {
								bucket : options["bucket"],
								method : options["method"],
								request_header : options["headers"],
								response_header : response.headers,
								statusCode : response.statusCode,
								status : "operations finished"
							});
						}
					}
				}
			});
			
			if (typeof options["action"] != "undefined" && options.srcFile) {
				var rstream;
				if (options["action"] == "UPLOAD_PART") {
					rstream = fs.createReadStream(options.srcFile, {
						start : options["seekTo"],
						end : (options["upload_length"] + options["seekTo"] - 1)
					});
				} else {
					rstream = fs.createReadStream(options.srcFile);
				}
	
				if (options["gzip"]) {
					rstream.pipe(oppressor(req));
				} else {
					rstream.pipe(req);
				}
			}
			
			if (typeof options["action"] != "undefined" && options.dstFile) {
				var wstream = fs.createWriteStream(options.dstFile);
				req.pipe(wstream);
			}
			callback(null, req);
		}], function(err, result) {
			if (err && cb) {
				cb(err, result);
			}
		}
	);
}

/**
 * make request URL
 * 
 * @param {Object} options (Optional)
 * @return {String}
 * @api private
 */
pro.makeURL = function(options) {
	var url = "";
	var params = [];
	url += config.use_ssl ? "https://" : "http://";
	url += (this._host + ":" + this._port);

	if (typeof options[config.OSS_BUCKET] === "string") {
		url += ("/" + options[config.OSS_BUCKET]);
	}
	if (typeof options[config.OSS_OBJECT] === "string") {
		url = url + "/" + options[config.OSS_OBJECT];
	}
	if (typeof options[config.OSS_PREFIX] === "string") {
		params.push("prefix=" + options[config.OSS_PREFIX]);
	}
	if (typeof options[config.OSS_MARKER] === "string") {
		params.push("marker=" + options[config.OSS_MARKER]);
	}
	if (typeof options[config.OSS_MAX_KEYS] !== "undefined") {
		params.push("max-keys=" + options[config.OSS_MAX_KEYS]);
	}
	if (typeof options[config.OSS_DELIMITER] === "string") {
		params.push("delimiter=" + options[config.OSS_DELIMITER]);
	}
	if (params.length > 0) {
		url = url + "?" + params.join("&");
	}
	if (typeof options[config.OSS_ACL] !== "undefined") {
		url += "?acl";
	}
	if (typeof options[config.OSS_OBJECT_GROUP] !== "undefined") {
		url += "?group";
	}
	if (typeof options[config.OSS_MULTI_PART] !== "undefined") {
		url += "?uploads";
	}
	if (typeof options[config.OSS_MULTI_DELETE] !== "undefined") {
		url += "?delete";
	}
	if (typeof options["partNumber"] !== "undefined") {
		url += ("?partNumber=" + options["partNumber"]);
	}
	if (typeof options["uploadId"] !== "undefined") {
		url += ("?uploadId=" + options["uploadId"]);
	}
	
	url = url.replace(/\?/g, "&").replace(/\&/, "?");

	return url;
}

/**
 * get canonicalizeResource for the auth
 * 
 * @param {Object} options (Optional)
 * @return {String}
 * @api private
 */
pro.canonicalizeResource = function(options) {
	var resource = "";

	if (typeof options[config.OSS_BUCKET] === "string") {
		resource = "/" + options[config.OSS_BUCKET];
	}
	if (typeof options[config.OSS_OBJECT] === "string") {
		resource = resource + "/" + options[config.OSS_OBJECT];
	}
	if (typeof options[config.OSS_ACL] !== "undefined") {
		resource += "?acl";
	}
	if (typeof options[config.OSS_OBJECT_GROUP] !== "undefined") {
		resource += "?group";
	}
	if (typeof options["partNumber"] !== "undefined") {
		resource += ("?partNumber=" + options["partNumber"]);
	}
	if (typeof options[config.OSS_MULTI_PART] !== "undefined") {
		resource += "?uploads";
	}
	if (typeof options["uploadId"] !== "undefined") {
		resource += ("?uploadId=" + options["uploadId"]);
	}
	if (typeof options[config.OSS_MULTI_DELETE] !== "undefined") {
		resource += "?delete";
	}
	
	resource = resource.replace(/\?/g, "&").replace(/\&/, "?");

	return resource;
}

/**
 * Perform the following:
 *  - ignore non-oss headers - lowercase fields - sort lexicographically - trim
 * whitespace between ":" - join with newline
 * 
 * @param {Object}
 *            headers
 * @return {String}
 * @api private
 */
pro.canonicalizeHeaders = function(headers) {
	var buf = [], fields = Object.keys(headers);
	for (var i = 0, len = fields.length; i < len; ++i) {
		var field = fields[i], val = headers[field], field = field.toLowerCase();
		if (0 !== field.indexOf("x-oss")) {
			continue;
		}
		buf.push(field + ":" + val);
	}
	return buf.sort().join("\n");
}

/**
 * get http request headers
 * 
 * @param {Object} options
 * @return {String}
 * @api private
 */
pro.getHeaders = function(options, cb) {
	var headers = {};
	var self = this;

	if (options["action"] == "UPLOAD_PART" && options.srcFile) {
		headers["content-type"] = mimetypes.lookup(path.extname(options.srcFile));
		self.fillHeaders(options, headers);
		cb(headers);
	} else {
		if (typeof options["action"] !== "undefined" && options.srcFile) {
			headers["content-type"] = mimetypes.lookup(path.extname(options.srcFile));
			async.waterfall([
				function checkFile(callback) {
					fs.stat(options.srcFile, function(err, stats) {
						if (err) {
							throw err;
						}
						callback(err, stats, options.srcFile);
					});
				}, function readFile(stats, file, callback) {
					if (stats.isFile()) {
						fs.readFile(file, "utf8", function(err, data) {
							callback(err, stats, data);
						});
					} else {
						throw new Error("file is not exist");
					}
				}, function fillFileData(stats, data, callback) {
					headers["Content-MD5"] = myUtil.md5(data);
					self.fillHeaders(options, headers);
					if (typeof headers["Content-Length"] == "undefined") {
						options["filesize"] = stats.size;
						headers["Content-Length"] = stats.size; 
					}
					callback(null, headers);
				}
			], function(err, result) {
				cb(headers);
			});
		} else {
			self.fillHeaders(options, headers);
			cb(headers);
		}
	}
}

/**
 * fill headers
 * 
 * @param {Object} options (Required)
 * @api private
 */
pro.fillHeaders = function(options, headers) {
	headers["Date"] = new Date().toGMTString();

	if (options[config.OSS_GROUP]) {
		headers["content-type"] = "txt/xml";
	}

	for (var key in options["headers"]) {
		headers[key] = options["headers"][key];
	}

	headers["Authorization"] = this.authorization(options, headers);
}

/**
 * Return an "Authorization" header value with the given `options` in the form
 * of "OSS <key>:<signature>"
 * 
 * @param {Object} options
 * @return {String}
 * @api private
 */

pro.authorization = function(options, headers) {
	var method = options["method"];
	var content_md5 = headers["Content-MD5"] || "";
	var content_type = headers["content-type"] || "";
	var date = headers["Date"];
	var canonicalizeHeader = this.canonicalizeHeaders(headers);
	var resource = this.canonicalizeResource(options);

	var params = [method, content_md5, content_type, date];

	if (canonicalizeHeader != "") {
		params.push(canonicalizeHeader);
	}
	params.push(resource);
	var string_to_sign = params.join("\n");

	return "OSS " + this._accessId + ":" + this.hmacSha1(string_to_sign);
};

/**
 * Simple HMAC-SHA1 Wrapper
 * 
 * @param signature
 * @return {String}
 * @api private
 */

pro.hmacSha1 = function(signature) {
	return crypto.createHmac("sha1", this._accessKey).update(signature).digest("base64");
};



/**
 * Get_Bucket get the list of objects in the bucket
 * 
 * @param {Object} options {bucket{required}:bucketName,prefix {optional}:prefix,max-keys{optional}:max-keys,marker{optional}:marker,delimiter{optional}:delimiter}
 * @return {Function} cb
 * @api public
 */
pro.list_object = function(options, callback) {
	check.bucketCheck(options);

	options["method"] = "GET";

	this.authRequest(options, callback);
}



/**
 * Put_Object put a object to a bucket in oss
 * 
 * @param {Object} options {bucket{required}:bucketName,object {required}:ossObjectName,{you can use / to create a directory in oss} srcFile {required}:localObjectPath}
 * @return {Function} cb
 * @api public
 */
pro.put_object = function(options, callback) {
	if (typeof options != "undefined") {
		if (typeof options["srcFile"] == "undefined") {
			throw new Error("put_object path is required");
		}
		check.bucketCheck(options);
		check.objectCheck(options);
	}

	options["method"] = "PUT";
	options["action"] = "UPLOAD_OBJECT";
	
	this.authRequest(options, callback);
}

/**
 * Get_Object get the object
 * 
 * @param {Object} options {bucket{required}:bucketName,object{required}:ossObjectName,dstFile{required}:downLoadObjectPath}
 * @return {Function} cb
 * @api public
 */
pro.get_object = function(options, callback) {
	if (typeof options != "undefined") {
		if (typeof options["dstFile"] == "undefined") {
			throw new Error("get_object path is required");
		}
		check.bucketCheck(options);
		check.objectCheck(options);
	}

	var self = this;
	self.head_object({
		bucket : options["bucket"],
		object : options["object"]
	}, function(err, result) {
		options["filesize"] = result["response_header"]["content-length"];
		options["method"] = "GET";
		self.authRequest(options, callback);
	});
}


/**
 * Head_Object get the meta infomation for the object like acl
 * 
 * @param {Object} options {bucket{required}:bucketName,object{required}:ossObjectName}
 * @return {Function} cb
 * @api public
 */
pro.head_object = function(options, callback) {
	check.bucketCheck(options);
	check.objectCheck(options);

	options["method"] = "HEAD";

	this.authRequest(options, callback);
}

/**
 * Delete_Object delete an object in the bucket
 * 
 * @param {Object} options {bucket{required}:bucketName,object{required}:ossObjectName}
 * @return {Function} cb
 * @api public
 */
pro.delete_object = function(options, callback) {
	check.bucketCheck(options);
	check.objectCheck(options);

	options["method"] = "DELETE";

	this.authRequest(options, callback);
}

/**
 * Delete_Objects delete many objects in the bucket
 * 
 * @param {Object} options {bucket{required}:bucketName,objects{required}:[ossObjectName1,ossObjectName2,...]}
 * @return {Function} cb
 * @api public
 */
pro.delete_objects = function(options, callback) {
	check.bucketCheck(options);

	options["method"] = "POST";
	options[config.OSS_MULTI_DELETE] = "delete";
	options["quiet"] = options["quiet"] || "false";

	var objects = options["objects"];
	if (objects.length != 0) {
		var xml_obj = {
			Quiet : options["quiet"],
			Object : []
		}

		for (var key in objects) {
			xml_obj.Object.push({Key : objects[key]});
		}

		var xml_content = data2xml("Delete", xml_obj);

		options["body"] = xml_content;
		options["Content-type"] = "application/xml";
		options["headers"] = {}

		options["headers"]["Content-Length"] = xml_content.length;
		options["headers"]["Content-MD5"] = crypto.createHash("md5").update(xml_content).digest("base64");

		this.authRequest(options, callback);
	} else {
		callback(null, "no objects in this bucket");
	}
}
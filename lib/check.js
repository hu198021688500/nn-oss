exports.bucketCheck = function(options) {
	if (typeof options != "undefined") {
		if (typeof options["bucket"] == "undefined") {
			throw new Error("bucket is required");
		}
	}
}

exports.objectCheck = function(options) {
	if (typeof options != "undefined") {
		if (typeof options["object"] == "undefined") {
			throw new Error("object is required");
		}
	}
}
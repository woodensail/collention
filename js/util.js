function addPrefix(data, prefixs, prefixHandles) {
	var flags = {};
	var _data = {};
	if (_.isString(prefixs)) {
		return _.mapKeys(data, function (v, k) {
			return prefixs + k
		});
	} else {
		_.each(prefixHandles, function (keyArray, handle) {
			_.each(keyArray, function (key) {
				_data[prefixs[handle] + key] = data[key];
				flags[key] = 1;
			});
		});
		_.each(_.difference(_.keys(data), _.keys(flags)), function (key) {
			_data[prefixs[0] + key] = data[key];
		});
		return _data;
	}
}
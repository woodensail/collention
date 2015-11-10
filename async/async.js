/*global exports*/
'use strict';
(function (factory) {
    if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else if (typeof define === 'function' && define.cmd) {
        define(function (require, exports, module) {
            module.exports = factory(jQuery);
        });
    } else if (typeof exports === 'object') {
        exports.async = factory();
    } else {
        // window.async=factory();
    }
}(function () {
    function async(generator) {
        return new Promise(function (resolve, reject) {
            var g = generator();

            function next(val) {
                var result = g.next(val);
                var promise = result.value;
                if (!result.done) {
                    if (promise instanceof Array) {
                        Promise.all(promise.map(autoPack)).then(next).catch(reject);
                    } else {
                        autoPack(promise).then(next).catch(reject);
                    }
                } else {
                    resolve(promise);
                }
            }

            try {
                next();
            } catch (e) {
                reject(e);
            }
        })
    }

    function autoPack(target) {
        // 包装$.ajax
        if (target.error) {
            return new Promise(function (resolve, reject) {
                target.done(resolve).error(reject);
            })
        } else {
            return target;
        }
    }

    return async;
}));


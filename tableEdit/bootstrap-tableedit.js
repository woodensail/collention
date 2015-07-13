/**
 * Created by sail on 2015/6/10.
 */
'use strict';
define(function (require, exports, module) {
    require('bootstrap');
    require('bootstrap-tableedit-css');

    $.fn.tableEdit = function (option) {
        var value, args = Array.prototype.slice.call(arguments, 1);
        this.each(function () {
            var $this = $(this), data = $this.data('tableEdit'), options = $.extend({}, TableEdit.DEFAULTS,
                $this.data(), typeof option === 'object' && option);
            if (typeof option === 'string') {
                value = data[option].apply(data, args);
            }
            if (!data) {
                $this.data('tableEdit', (data = new TableEdit(this, options)));
            }
        });
        return value === undefined ? this : value;
    };

    var TableEdit = function (jq, options) {
        this.$table = $(jq);
        this.$main = this.$table.parents('.bootstrap-table').eq(0);
        this.options = options;
        this.init();
    };

    TableEdit.EVENTS = {
        'hidden.bs.tableedit' : 'onHidden',
        'row-end.bs.tableedit': 'onRowEnd',
        'change.bs.tableedit' : 'onChange'
    };

    TableEdit.prototype.init = function () {
        var _this = this;
        _this.initListener();
        _this.changList = {};
        _this.columns = {};
        _.each(this.options.columns, function (column) {
            _this.initColumns(column);
        });

        var lastColumn;
        _this.orderedColumns = [];
        _this.$table.find('thead').find('th').each(function (i) {

            var column = _this.columns[$(this).data('field')];
            if (undefined === column) {
                return;
            }
            if (undefined !== lastColumn) {
                column.prev = lastColumn;
                lastColumn.next = column;
            }
            _this.orderedColumns.push(column);
            column.index = i;
            lastColumn = column;
        });
    };
    TableEdit.prototype.initListener = function () {
        var _this = this;
        _this.$mask = $('<div style="position: absolute;left: 0;top:0;z-index: 1"></div>').appendTo($('body'));
        _this.$table.on('click-cell.bs.table', function (e, field, value, item, $td) {
            _this.editing = true;
            _this.value = value;
            _this.index = $td.parent().data('index');
            _this.columns[field].show($td, value);
            _this.column = _this.columns[field];
            _this.$mask.width($(document).width()).height($(document).height()).off('click').on('click', function () {
                _this.hide();
            });
            _this.column.focus();
        });
        _this.$main.on('keydown', function (e) {
            switch (e.keyCode) {
                case 27:
                    //_this.hide();
                    _this.show(_this.column);
                    break;
                case 37:
                    _this.column.prev ? _this.show(_this.column.prev) : null;
                    break;
                case 39:
                    _this.column.next ? _this.show(_this.column.next) : null;
                    break;
                default :
                    break;
            }
        });
    };
    TableEdit.prototype.initColumns = function (column) {
        var _this = this;
        var Type = TableEdit.types[column.type];
        var options = $.extend(true, {}, Type.DEFAULTS, column);
        var _column = new Type();
        _column.options = options;
        _column.root = _this;
        _column.$pop = _column.init().appendTo(_this.$main);
        _this.columns[options.field] = _column;
    };

    TableEdit.prototype.hide = function () {
        var _this = this;
        if (!_this.editing) {
            return;
        }
        _this.editing = false;
        if (_this.saveChange(_this.value, _this.index, _this.column.options.valueField)) {
            _this.$table.bootstrapTable('updateCell', {
                rowIndex  : _this.index,
                fieldName : _this.column.options.valueField,
                fieldValue: _this.value
            });
        }
        _this.$mask.width(0).height(0);
        _this.column.$pop.css({visibility: 'hidden'});
        _this.trigger('hidden', _this.value);
    };


    TableEdit.prototype.show = function (column, line) {
        var _this = this;

        _this.hide();
        if (undefined === column) {
            return;
        }
        var index = (undefined === line ? _this.index : line);
        var field = column.options.field;
        var item = _this.$table.bootstrapTable('getData')[index];
        if (undefined === item) {
            return;
        }
        _this.editing = true;
        _this.index = index;
        var value = item[field];
        var $td = _this.$table.find('tbody tr:eq(' + _this.index + ') td:eq(' + column.index + ')');


        _this.value = value;
        _this.index = _this.index;
        _this.columns[field].show($td, value);
        _this.column = column;
        _this.$mask.width($(document).width()).height($(document).height()).off('click').on('click', function () {
            _this.hide();
        });
        _this.column.focus();
        return column;
    };

    TableEdit.prototype.goNext = function () {
        if (undefined === this.show(this.column.next)) {
            this.trigger('row-end');
        }
    };

    TableEdit.prototype.trigger = function (name) {
        var args = Array.prototype.slice.call(arguments, 1);
        name += '.bs.tableedit';
        this.column[TableEdit.EVENTS[name]].apply(this.column, args);
        this.$table.trigger($.Event(name), args);
    };

    TableEdit.prototype.startEdit = function (line, column) {
        line = undefined === line ? 0 : line;
        column = undefined === this.columns[column] ? this.orderedColumns[0] : this.columns[column];
        this.show(column, line)
    };

    TableEdit.prototype.saveChange = function (newValue, rowIndex, field) {
        var _this = this, node, oldValue, tableValue;
        if (null == _this.changList[rowIndex]) {
            _this.changList[rowIndex] = {};
        }
        node = _this.changList[rowIndex][field];
        if (null == node) {
            oldValue = _this.$table.bootstrapTable('getData')[rowIndex][field];
            tableValue = oldValue;
        } else {
            oldValue = node.old;
            tableValue = node.newValue;
        }
        if (newValue === oldValue) {
            _this.changList[rowIndex][field] = null;
        } else {
            _this.changList[rowIndex][field] = {
                newValue: newValue,
                oldValue: oldValue
            };
        }
        if (tableValue !== newValue) {
            _this.trigger('change', newValue, rowIndex, field, _this.data);
            return true;
        }
    };

    var TypeBase = function () {

    };
    TypeBase.DEFAULTS = {};
    TypeBase.prototype = {
        init    : function () {
        },
        show    : function () {
        },
        onHidden: function (value) {
        },
        onRowEnd: function () {
            this.root.show(this.root.orderedColumns[0], this.root.index + 1);
        },
        onChange: function (value, rowIndex, field) {
        }
    };
    var TypeInput = function () {

    };
    TypeInput.DEFAULTS = $.extend({}, TypeBase.DEFAULTS, {});
    TypeInput.prototype = $.extend(new TypeBase(), {
        init : function (target, value) {
            var _this = this;
            var jq = $('<input class="tableedit-input" style="z-index: 2; position: absolute;visibility:hidden"/>').click(function (e) {
                e.stopPropagation();
            });
            jq.keyup(function (e) {
                _this.root.value = jq.val();
                if (e.keyCode === 13) {
                    _this.root.goNext();
                }
            });
            return jq;
        },
        show : function (target, value) {
            var _this = this;
            _this.$pop.val(value).offset(target.offset()).outerWidth(target.outerWidth()).outerHeight(target.outerHeight()).css({visibility: 'visible'});
        },
        focus: function () {
            this.$pop.focus();
        }

    });
    var TypeTable = function () {

    };
    TypeTable.DEFAULTS = $.extend({}, TypeBase.DEFAULTS, {});
    TypeTable.prototype = $.extend(new TypeBase(), {
        init    : function () {
            var _this = this;
            var $div = $('<div style="z-index: 2; position: absolute;border:0;height: 0;visibility:hidden" class="tableedit-table-container">' +
            '<div class="input-group"><input class="tableedit-table-input form-control"/> <span class="input-group-btn">' +
            '<button class="btn btn-default tableedit-table-input-btn" type="button"><span class="glyphicon glyphicon-chevron-down"></span></button></span></div>' +
            '<div class="tableedit-table-table-container"><table/></div></div>').click(function (e) {
                e.stopPropagation();
            });
            _this.$table = $div.find('table');
            if (_this.options.typeOptions.url) {
                _this.options.typeOptions.table.data = undefined;
            } else {
                if (!_.isFunction(_this.options.typeOptions.data)) {
                    _this.options.typeOptions.table.data = _this.options.typeOptions.data
                }
            }
            _this.$table.bootstrapTable($.extend({
                maintainSelected    : true,
                pageSize            : 3,
                pagination          : true,
                formatRecordsPerPage: function (pageNumber) {
                    return '' + pageNumber + '';
                },
                formatShowingRows   : function (pageFrom, pageTo, totalRows) {
                    return pageFrom + '-' + pageTo + '/' + totalRows;
                }
            }, _this.options.typeOptions.table)).on('click-cell.bs.table', function (e, field, value, item, $td) {
                _this.setValue(item);
                e.stopPropagation();
            }).on('dbl-click-cell.bs.table', function (e, field, value, item, $td) {
                _this.root.goNext();
                e.stopPropagation();
            });

            $div.find('.tableedit-table-input-btn').click(function () {
                if (!_this.tableFlag) {
                    _this.dropDown($(this).val());
                } else {
                    _this.setValue(_this.selected);
                    _this.hideTable();
                }
                _this.focus();
            });


            _this.tableFlag = false;
            var $input = $div.find('.tableedit-table-input');
            $input.parent().keyup(function (e) {
                if (e.keyCode === 13) {
                    var pastString = _this.qString;
                    var qString = $input.val();
                    _this.qString = qString;
                    if (!qString) {
                        _this.setValue();
                        _this.root.goNext();
                    } else if (!_this.tableFlag || (pastString !== qString)) {
                        _this.dropDown(qString);
                        $input.focus();
                    } else {
                        _this.setValue(_this.selected);
                        _this.root.goNext();
                    }

                } else if (e.keyCode === 38) {
                    _this.select(undefined === _this.selected ? 0 : _this.selected - 1);
                } else if (e.keyCode === 40) {
                    _this.select(undefined === _this.selected ? 0 : _this.selected + 1);
                }
            });


            return $div
        },
        show    : function (target) {
            var _this = this;
            _this.tableFlag = false;
            var $input = _this.$pop.find('.tableedit-table-input');
            _this.$pop.offset(target.offset()).css({visibility: 'visible'});
            $input.val(target.text().trim());
            $input.parent().outerWidth(target.outerWidth());
            $input.outerHeight(target.outerHeight());
            $input.next().children().outerHeight(target.outerHeight());
            var $tableContainer = _this.$pop.find('.tableedit-table-table-container').css({visibility: 'hidden'});

            //当typeOptions中的data属性为函数时，调用该函数取得当前行所对应的data属性，并填入table中
            if (_.isFunction(_this.options.typeOptions.data)) {
                _this.$table.bootstrapTable('load', _this.options.typeOptions.data(_this.root.index))
            }

            _this.showTable = function () {
                var height = $tableContainer.outerHeight();
                var inputHeight = target.outerHeight();
                var top = target.offset().top - $(document).scrollTop();
                var offset = $input.offset();
                if ((top + inputHeight + height) > $(window).height() && (top - height) > 0) {
                    offset.top -= height;
                } else {
                    offset.top += inputHeight;
                }
                _this.selected = undefined;
                $tableContainer.offset(offset).css({visibility: ''});
                _this.tableFlag = true;
            };
            _this.hideTable = function () {
                $tableContainer.css({visibility: 'hidden'});
                _this.tableFlag = false;
            }
        },
        focus   : function () {
            this.$pop.find('.tableedit-table-input').focus();
        },
        dropDown: function (qString) {
            var _this = this;
            if (_this.options.typeOptions.url) {
                var params = _this.options.typeOptions.params;
                if(_.isFunction(params)){
                    params=params();
                }
                var data = $.extend({}, params);
                _.each(_this.options.typeOptions.queryField, function (v) {
                    data[v] = qString;
                });
                var tableOpts = _this.$table.bootstrapTable('getOptions');
                tableOpts.url = _this.options.typeOptions.url;
                tableOpts.sidePagination = 'server';
                tableOpts.queryParams = function (params) {
                    return $.extend(params, data);
                };
                _this.$table.bootstrapTable('refresh');
                _this.showTable();
            } else {
                _this.$table.data('bootstrap.table').searchText = qString;
                _this.$table.bootstrapTable('filterBy', {});
                _this.showTable();
            }
        },
        select  : function (index) {
            var $tableContainer = this.$pop.find('.tableedit-table-table-container');
            var target = $tableContainer.find('.table tr[data-index="' + index + '"]');

            if (0 !== target.length) {
                this.selected = index;
                $tableContainer.find('.table tr.selected').removeClass('selected');
                target.eq(0).addClass('selected');
            }
        },
        setValue: function (v) {
            if (_.isNumber(v)) {
                v = this.$table.bootstrapTable('getData')[v];
            }
            if (undefined === v) {
                this.root.value = null;
                this.$pop.find('.tableedit-table-input').val('');
            } else {
                this.root.value = v[this.options.typeOptions.valueField];
                this.root.data=v;
                this.$pop.find('.tableedit-table-input').val(v[this.options.typeOptions.textField]);
            }
        }
    });

    TableEdit.DEFAULTS = {
        type: 'input'
    };
    TableEdit.METHODS = {
        type: 'input'
    };
    TableEdit.types = {
        'input': TypeInput,
        'table': TypeTable
    }
});
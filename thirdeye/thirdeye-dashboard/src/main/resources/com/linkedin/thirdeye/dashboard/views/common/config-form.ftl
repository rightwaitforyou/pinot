<div class="title-box uk-clearfix">

    <form class="time-input-form uk-form uk-form-stacked uk-clearfix">
        <button class="select-button uk-button" type="button" style="width: 200px;"><span>Select Filters </span><i class="uk-icon-caret-down"></i></button>
        <div class="dimension-combination">
            <a class="close" href="#">
                <i class="uk-icon-close"></i>
            </a>
            <br>
            <table>
                <tr>
                    <td>Select Filters: </td>
                <#list (metricView.view.metricTables)!metricTables as metricTable>
                    <#assign dimensions = metricTable.dimensionValues>
                    <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
                    <#list dimensions?keys as dimensionName>
                        <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                        <td style="position:relative;">
                            <span>${dimensionDisplay}:</span><br>

                            <button type="button" class="dimension-selector uk-button">Select Values <i class="uk-icon-caret-down"></i> </button>
                            <div class="hidden" style="position:absolute; top:50px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                                <ul style="list-style-type: none; padding-left:0; width:250px;">
                                    <#list dimensionValuesOptions[dimensionName]![] as dimensionValue>
                                        <li style="overflow:hidden;">
                                            <#assign dimensionValueDisplay=dimensionValue?html>
                                            <#if dimensionValue=="">
                                                <#assign dimensionValueDisplay="UNKNOWN">
                                            <#elseif dimensionValue=="?">
                                                <#assign dimensionValueDisplay="OTHER">
                                            </#if>
                                            <input class="panel-dimension-option" type="checkbox" dimension-name="${dimensionName}"/>${dimensionValueDisplay?html}
                                        </li>
                                    </#list>

                                </ul>
                            </div>

                        </td>
                    </#list>
                </#list>
                </tr>
            </table>
        </div>

        <div class="uk-margin-small-top uk-margin-bottom">
            <div id="time-input-form-error" class="uk-alert uk-alert-danger hidden">
                <p></p>
            </div>
        <#--<div id="dimension-input" class="uk-display-inline-block" style="position: relative;">
            <button class="select-button uk-button" type="button" style="width: 225px;"><span>Change Dimension Query </span><i class="uk-icon-caret-down"></i></button>
            <div class="hidden" style="width: 100%; z-index:100; position:absolute; top:30px; left:0; background-color: #f5f5f5; border: 1px solid #ccc; height: auto;">
                <span class="multiselect-close"><a href="#"><i style="position: relative;
                left: 205px; color:#444;" class="uk-icon-close"></i></a></span>

                <ul class="multiselect-panel" style="z-index: 100; list-style-type: none; padding-left: 0px; margin-bottom: 0px;">
                     <#list (metricView.view.metricTables)!metricTables as metricTable>
                    <#assign dimensions = metricTable.dimensionValues>
                    <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
                    <#list dimensions?keys as dimensionName>
                        <#assign dimensionValue = dimensions[dimensionName]>
                        <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>
                        <li class="multiselect-optgroup-label" style="display: inline;" ><a href="#"  style="  display: block; padding: 3px; margin: 1px 0; text-align: center; text-decoration: none; color: #444; background-image: -webkit-linear-gradient(top,#eee,#999);background-image: linear-gradient(to bottom,#eee,#999);">${dimensionDisplay}:</a>
                            <ul style="list-style-type: none; padding-left: 0px;">
                                <li class="multiselect-optgroup" style="display: inline-block; width: 100px;">
                                    <input id="${DIMENSIONINDEX}_${dimensions[dimensionName]}" type="checkbox" value="${dimensions[dimensionName]}"/><span>${dimensions[dimensionName]}</span></label>
                                </li>
                            </ul>
                        </li>
                    </#list>
                </#list>
                </ul>
            </div>
            </div>-->


            <div class="uk-display-inline-block" style="position: relative;">
                <button id="time-input-metrics" type="button" class="uk-button" style="width: 200px;">Select Metrics <i class="uk-icon-caret-down"></i> </button>
                <div id="time-input-metrics-panel" class="hidden" style="position:absolute; top:30px; left:0px; z-index:100; background-color: #f5f5f5; border: 1px solid #ccc; padding:5px;">
                    <ul style="list-style-type: none; padding-left:0; width:250px;">
                    <#list collectionSchema.metrics as metric>
                        <li style="overflow:hidden;">
                            <input class="panel-metric" type="checkbox" value="${metric}"/>${collectionSchema.metricAliases[metric_index]!metric}
                        </li>
                    </#list>
                    </ul>
                </div>
            </div>

            <#if (dimensionView.type == "HEAT_MAP" || dimensionView.type == "MULTI_TIME_SERIES")>
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Start Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input id="time-input-form-baseline-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>
            </#if>

            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    End Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input id="time-input-form-current-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>

            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Granularity
                </label>
                <div  class="uk-button-group" data-uk-button-radio>
                    <button type="button" class="baseline-aggregate uk-button" unit="HOURS" value="3600000" >hour(s)</button>
                    <button type="button" class="baseline-aggregate uk-button uk-active" unit="DAYS" value="86400000" >day(s)</button>
                </div>
            </div>

            <#if (dimensionView.type == "HEAT_MAP" || dimensionView.type == "TABULAR")>
            <div id="time-input-form-moving-average" class="uk-form-label" style="display: inline-block">Overlay<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Moving Average:</span>
                <i class="uk-icon-caret-down"></i>
                    <select id="time-input-moving-average-size">
                        <option class="uk-button" unit="WoW" value="">None</option>
                        <option class="uk-button" unit="WoW" value="7">WoW</option>
                        <option class="uk-button" unit="Wo2W" value="14" >Wo2W</option>
                        <option class="uk-button" unit="Wo4W" value="28">Wo4W</option>
                    </select>
                </div>
            </div>
            </#if>

            <div class="uk-display-inline-block uk-margin-right">
                <button type="submit" class="time-input-form-submit uk-button uk-button-small uk-button-primary ">Go</button>
            </div>
        </div>
    </form>


    <div id="current-view-settings" class="uk-clearfix" style="padding-right:20px;">
        <!-- Filters Applied -->
    <#list (metricView.view.metricTables)!metricTables as metricTable>
        <#assign dimensions = metricTable.dimensionValues>
        <#assign dimensionAliases = (metricView.view.dimensionAliases)!dimensionAliases>
        <ul class="filters-applied uk-float-right" style="display: inline-block;">Filters Applied:
            <#list dimensions?keys as dimensionName>
                <#assign dimensionValue = dimensions[dimensionName]>
                <#assign dimensionDisplay = dimensionAliases[dimensionName]!dimensionName>

                <#if dimensionValue == "*">
                <#--<span>${dimensionDisplay}:</span><br> ALL-->
                <#elseif dimensionValue == "?">
                    <li>
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span> OTHER</a>
                    </li>
                <#else>
                    <li>
                        <a href="#" class="dimension-link" dimension="${dimensionName}"><span>${dimensionDisplay}:</span> ${dimensions[dimensionName]}</a>
                    </li>
                </#if>

            </#list>
        </ul>
    </#list>


       <#if (dimensionView.type == "TABULAR")>
        <div class="uk-display-inline-block uk-float-right uk-margin-small">
            <div data-uk-button-checkbox>
                <button type="button" id="funnel-cumulative" class="uk-button">Cumulative</button>
            </div>
        </div>
        <!-- Metric selection dropdown -->
        <#elseif (dimensionView.type == "HEAT_MAP")>
        <div class="uk-display-inline-block uk-float-right uk-margin-small">Metric:<br>
            <div  class="uk-button uk-form-select" data-uk-form-select>
                <span>Metric</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-metric-selector" class="section-selector">
                    <#list dimensionView.view.metricNames as metric>
                        <option value="${metric}">${metric}</option>
                    </#list>
                </select>
            </div>
        </div>
        <#elseif (dimensionView.type == "MULTI_TIME_SERIES")>
        <div class="uk-display-inline-block uk-float-right uk-margin-small">Dimension:<br>
            <div  class="uk-button uk-form-select uk-right" data-uk-form-select>
                <span>Dimension</span>
                <i class="uk-icon-caret-down"></i>
                <select id="view-dimension-selector" class="section-selector">
                    <#list dimensionView.view.dimensions as dimension>
                        <option value="${dimension}">${dimension}</option>
                    </#list>
                </select>
            </div>
        </div>
        </#if>


        <#if (dimensionView.type == "HEAT_MAP")>
        <ul class="heatmap-tabs uk-tab" data-uk-tab>
            <li class="uk-active">
                <a href="#">Heatmap</a>
            </li>
            <li>
                <a href="#">Datatable</a>
            </li>
        </ul>
        <#elseif (dimensionView.type == "TABULAR")>
        <ul class="uk-tab funnel-tabs" data-uk-tab>
            <li class="uk-active">
                <a href="#">Summary</a>
            </li>
            <li>
                <a href="#">Details</a>
            </li>
        </ul>
        </#if>
    </div>

</div>
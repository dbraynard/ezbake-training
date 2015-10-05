<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1" %>
<%@ page import="ezbake.configuration.EzConfiguration,ezbake.security.client.EzbakeSecurityClient" %>
<!--
 * Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. -->
<html>
<head>

    <!-- Bootstrap -->
    <link href="css/bootstrap.css" rel="stylesheet">
    <!--<link href="css/bootstrap.css.map" rel="stylesheet">-->
    <!--<link href="css/bootstrap-theme.css.map" rel="stylesheet">-->
    <link href="css/bootstrap-theme.css" rel="stylesheet">

    <!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
    <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
    <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
    <![endif]-->

    <script src="jquery-1.10.2.js"></script>

    <script type="text/javascript">
        $(function () {
            $('#ezmongoContent').keyup(function () {
                var count = $(this).val().length;
                $('#characters').text(count);
                //console.log("# char: " + count);
            });

            $("#ezmongoSearch").keyup(function (event) {
                //console.log("searchText keyup: " + event.keyCode);
                if (event.keyCode == 13) {
                    $("#searchButton").click();
                }
            });
        });

        function insertEzMongo() {
            if (!validateNumCharacters()) {
                return;
            }

            var content = $("#ezmongoContent").val();

            $("#insertResult").attr('class', 'text-success');
            $("#insertResult").html("Calling the Content Publisher service, please wait...");
            $("#insertButton").prop("disabled", true);

            $.ajax({
                url: 'MongoDbServlet', type: "POST", data: {
                    action: 'insertText',
                    Content: content
                }, success: function (result) {
                    $("#insertResult").attr('class', 'text-success');
                    $("#insertResult").html(result);
                    $("#insertButton").prop("disabled", false);
                }, error: function (xhr, status, result) {
                    var err = xhr.responseText;
                    console.log("ERROR in insert: " + err);
                    $("#insertResult").attr('class', 'text-danger');
                    $("#insertResult").html(err);
                    $("#insertButton").prop("disabled", false);
                }
            });
        }


        function searchEzMongo() {
            var searchText = $("#searchText").val();

            $.ajax({
                url: 'MongoDbServlet', type: "POST", data: {
                    action: 'searchEzMongo', searchText: searchText
                }, success: function (result) {
                    $("#searchResult tbody").html(result);
                }, error: function (xhr, status, result) {
                    var err = xhr.responseText;
                    console.log("ERROR in search: " + err);
                    $("#searchResult tbody").html(err);
                }
            });

        }

        function validateNumCharacters() {
            var count = $('#ezmongoContent').val().length;

            if (count > 140) {
                alert("You have exceeded 140 characters: " + count);
                return false;
            }

            return true;
        }
    </script>
</head>
<body>

<div id="auths" style="background-color: gold">
    <% try {
        EzbakeSecurityClient securityClient = new EzbakeSecurityClient(new EzConfiguration().getProperties());
        out.println(securityClient.fetchTokenForProxiedUser().getTokenPrincipal().getPrincipal());
        out.println(securityClient.fetchTokenForProxiedUser().getAuthorizations());
    } catch (Exception e) {
        e.printStackTrace();
    }%>
</div>
<div id="warning" style="background-color: red; text-align:center; color:white">
    Please note that the security level visibilities in this app are all test data - not real classified data.
</div>

<div id="container">
    <div class="panel panel-default" id="content">
        <h3 class="panel-title">Datasets Webapp Demo</h3>

        <!-- Insert Text -->
        <div class="panel-body">
            <label>Insert Text</label>

            <textarea id="ezmongoContent" class="form-control" rows="4" cols="40"
                      placeholder="Enter text here"></textarea>
            <em class="hint" style="margin-top: 10px;">Number of characters:
                <div id="characters" class="numCharacters">0</div>
            </em>
            <em class="hint" style="margin-top: 10px;">(max 140 characters)</em>
            <span class="input-group-btn">
                <button class="btn btn-info btn-large" id="insertButton" type="button" name="insert"
                        onClick="insertEzMongo()">Submit
                </button>
            </span>

            <!-- Insert ezmongo output msg -->
            <div id="insertResult" class="text-success"></div>
        </div>

        <!-- Search EzMongo -->
        <ul class="list-group">
            <li class="list-group-item">
                <label>Search Text</label>
                <em class="hint" style="margin-top: 10px;">Enter search term (stop words such as 'the', 'an', 'a' are
                    not supported):</em>
                <input id="searchText" type="text" class="form-control" placeholder=""></br>

            <span class="input-group-btn">
                <button class="btn btn-info btn-large" id="searchButton" type="button" name="search"
                        onClick="searchEzMongo()">Search
                </button>
            </span>
            </li>
            <li class="list-group-item">
                <!-- Search result output list -->
                <table id="searchResult" class="table table-condensed">
                    <thead>
                        <tr>
                            <th class="text">text</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- empty on initial page load. Will get populated by search results using AJAX. -->
                    </tbody>
                </table>
            </li>
        </ul>
    </div>
</div>

<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js"></script>
<!-- Include all compiled plugins (below), or include individual files as needed -->
<script src="js/bootstrap.min.js"></script>
</body>
</html>

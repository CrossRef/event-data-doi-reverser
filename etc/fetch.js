"use strict";

console.error = function () {
    require("system").stderr.write(Array.prototype.join.call(arguments, ' ') + '\n');
};

// Requested resource => response code.
var resourceResponseCodes = {};

var finalUrl = "";
var timeout = null;

var requested = null;
var finished = false;

var path = [];
var log = [];

var page = require('webpage').create();
var system = require('system');
var fs = require('fs');

page.onError = function(message, trace) {
  stderr.write("Script error:" + message);
};

page.customHeaders = {
  "User-Agent": "Crossref Thamnophilus labs@crossref.org (+http://labs.crossref.org)"
};

var resetTimeout = function(callback, time) { 
    // system.stderr.write("resetTimeout");

    window.clearTimeout(timeout);
    timeout = window.setTimeout(function() {
        var numRedirects = path.length;
        var lastUrl = null;
        if (numRedirects > 0) {
            lastUrl = path[numRedirects-1];
        }
        callback(lastUrl, window.path);
    }, time);
};

var navigatePage = function (doi, callback) {
    // system.stderr.write("navigatePage");

    requested = null;
    finished = false;
    resetTimeout(callback, 2000);

  page.onResourceReceived = function(response) {
    if (response.stage !== "end") {
        return;
    }

    if (response.status) {
        resourceResponseCodes[response.url] = response.status;
    }
  };

    page.onNavigationRequested = function(url, type, willNavigate, mainWindow) {
        log.push(url);

        if (mainWindow && url != finalUrl) {
            window.path.push(url);
            requested = url;
            // 2 seconds from the page request to load and possibly re-navigate.
            resetTimeout(callback, 2000);
        }
    };

    page.onResourceRequested = function(requestData, networkRequest) {
      var match = requestData.url.match(/\.(jpg|gif|png)/g);
      if (match !== null) {
        networkRequest.abort(); 
      }
    };

    page.open(doi, function(status) {
      // Shorter timeout once things are loaded in case some JS wants to run.
      resetTimeout(callback, 1);
    });
};

// Master 10 second timeout for if things really go south.
window.setTimeout(function() {
    system.stderr.write("ERROR: Ten second timeout. Aborting.");
    phantom.exit(1);
}, 10000);

// Read a line that contains a URL
var inputUrl = system.stdin.readLine();
// system.stderr.write("1");
if (inputUrl === undefined) {
    // system.stderr.write("5");
    system.stderr.write("ERROR: Didn't get an input URL.\n");
    phantom.exit(1);
} else {
    // system.stderr.write("2");
    // Callback with the ultimate PID plus the path that we followed to get here.
    navigatePage(inputUrl, function (pid, path) {
        var response = {
            "path": window.path.map(function(url) {
                // Fake 503 when not supplied. TODO: Find reason!
                var status = window.resourceResponseCodes[url] || 503;
                return [url, status];
            })};
        
        system.stdout.write(JSON.stringify(response));
        phantom.exit(0);
    });
}


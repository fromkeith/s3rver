// based off of: https://github.com/jpettersson/node-ordered-merge-stream

var from = require('from');

module.exports = function(streams) {
  var isEmptyStream = false;
  var outStream = from(function getChunk(count, next) {});

  var streamQueue = [];
  var currentStream = 0;

  // Emit everything available so far in the queue
  function emitData() {
    if(currentStream === streamQueue.length) {
      outStream.emit('end');
      return;
    }
    var dataQueue = streamQueue[currentStream].dataQueue;
    for(;dataQueue.length > 0;) {
      var data = dataQueue.shift();
      if(!!data) {
        outStream.emit('data', data);
        return emitData();
      }
    }
  }

  function processStream(index, stream) {
    stream.on('data', function(data) {

      streamQueue[index].dataQueue.push(data);
      emitData();
    });
    stream.on('end', function() {
      currentStream++;
      if (currentStream < streams.length) {
        streams[currentStream].resume(); // start next stream
      }

      // The stream was empty and didn't send any data
      if(streamQueue[index].length === 0) {
        isEmptyStream = true;
        streamQueue[index].dataQueue.push(null);
      }
      emitData();
    });
  }


  for(var i=0;i<streams.length;i++) {
    streamQueue.push({dataQueue: []});
  }

  for(var j=0;j<streams.length;j++) {
    processStream(j, streams[j]);
  }
  if (streams.length > 0) {
    streams[0].resume();
  }

  return outStream;
};
var graph_model = null;
var plasma = null;

(function($){

  var graph_renderer = function(canvas){
    var canvas = $(canvas).get(0);
    var ctx = canvas.getContext("2d");
    var graph;
    var node_handler = null;

    // Return the intersection point of two lines
    var intersect_line_line = function(p1, p2, p3, p4) {
      var denom = ((p4.y - p3.y)*(p2.x - p1.x) - (p4.x - p3.x)*(p2.y - p1.y));
      if (denom === 0) return false // lines are parallel
      var ua = ((p4.x - p3.x)*(p1.y - p3.y) - (p4.y - p3.y)*(p1.x - p3.x)) / denom;
      var ub = ((p2.x - p1.x)*(p1.y - p3.y) - (p2.y - p1.y)*(p1.x - p3.x)) / denom;

      if (ua < 0 || ua > 1 || ub < 0 || ub > 1)  {
        return false 
      } else {
        return arbor.Point(p1.x + ua * (p2.x - p1.x), p1.y + ua * (p2.y - p1.y));
      }
    }

    // Return the intersection point of a line and a box
    var intersect_line_box = function(p1, p2, boxTuple) {
      var p3 = {x: boxTuple[0], y: boxTuple[1]},
          w = boxTuple[2],
          h = boxTuple[3]

      var tl = {x: p3.x, y: p3.y};
      var tr = {x: p3.x + w, y: p3.y};
      var bl = {x: p3.x, y: p3.y + h};
      var br = {x: p3.x + w, y: p3.y + h};

      return intersect_line_line(p1, p2, tl, tr) ||
            intersect_line_line(p1, p2, tr, br) ||
            intersect_line_line(p1, p2, br, bl) ||
            intersect_line_line(p1, p2, bl, tl) ||
            false;
    }

    var renderer = {
      selected_node: null,
      node_boxes: {},

      on_node_clicked: function(handler) {
        node_handler = handler;
      },

      init: function(system){
        //
        // the particle system will call the init function once, right before the
        // first frame is to be drawn. it's a good place to set up the canvas and
        // to pass the canvas size to the particle system
        //
        // save a reference to the particle system for use in the .redraw() loop
        graph = system;

        // inform the system of the screen dimensions so it can map coords for us.
        // if the canvas is ever resized, screenSize should be called again with
        // the new dimensions
        graph.screenSize(canvas.width, canvas.height) ;
        graph.screenPadding(80); // leave an extra 80px of whitespace per side
        
        // set up some event handlers to allow for node-dragging
        renderer.initMouseHandling();
      },

      draw_edge: function(ctx, edge, pt1, pt2) {
        var tail = intersect_line_box(pt1, pt2, renderer.node_boxes[edge.source.name])
        var head = intersect_line_box(tail, pt2, renderer.node_boxes[edge.target.name])
        ctx.save();
        ctx.strokeStyle = "rgba(0,0,0,0.666)";
        ctx.lineWidth = 1.0;
        ctx.beginPath();
        ctx.moveTo(tail.x, tail.y);
        ctx.lineTo(head.x, head.y);
        ctx.stroke();
        ctx.restore();

        var arrow_length = 10;
        var arrow_width = 4;
        ctx.save();
        ctx.fillStyle = "rgba(0,0,0,0.666)";
        ctx.translate(head.x, head.y);
        ctx.rotate(Math.atan2(head.y - tail.y, head.x - tail.x));
        ctx.beginPath();
        ctx.moveTo(-arrow_length, arrow_width);
        ctx.lineTo(0, 0);
        ctx.lineTo(-arrow_length, -arrow_width);
        ctx.lineTo(-arrow_length * 0.8, -0);
        ctx.fill();
        ctx.restore();

        if(edge.data.label) {
          //var center = head.subtract(tail).divide(2).add(tail);
          var center = pt2.subtract(pt1).divide(2).add(pt1);
          ctx.save();
          ctx.strokeStyle = "rgba(0,0,0, .666)";
          ctx.strokeText(edge.data.label, center.x, center.y); 
          ctx.restore();
        }
      },

      draw_node: function(ctx, node, pt) {
        // determine the box size and round off the coords if we'll be 
        // drawing a text label (awful alignment jitter otherwise...)
        var label = "" + (node.data.label || node.name.substring("5, 9"));
        var width = ctx.measureText(label).width + 12;
        var height = 18;
        pt.x = Math.floor(pt.x)
        pt.y = Math.floor(pt.y)
        renderer.node_boxes[node.name] = [pt.x-width/2, pt.y-width/2, width, height];

        var fill = "rgb(40, 40, 40)";
        if(plasma.graph.renderer.selected_node == node.name) {
          fill = "red";
        } 

        // Rectangle centered at point pt
        ctx.save();
        ctx.fillStyle = fill;
        ctx.fillRect(pt.x-width/2, pt.y-height/2, width, height);

        ctx.font = "12px Helvetica";
        ctx.textAlign = "center";
        ctx.fillStyle = "white";
        ctx.fillText(label, pt.x, pt.y+4);
        /*
        if (node.data.color=='none') {
          ctx.fillStyle = '#333333';
        }
        */

        ctx.restore();
      },

      redraw: function() {
        ctx.fillStyle = "white"
        ctx.fillRect(0,0, canvas.width, canvas.height)
        
        graph.eachNode(function(node, pt) {
          renderer.draw_node(ctx, node, pt);
        });

        graph.eachEdge(function(edge, start, end){
          renderer.draw_edge(ctx, edge, start, end);
        });
      },
      
      initMouseHandling: function(){
        // no-nonsense drag and drop (thanks springy.js)
        var dragged = null;

        // set up a handler object that will initially listen for mousedowns then
        // for moves and mouseups while dragging
        var handler = {
          clicked: function(e){
            var pos = $(canvas).offset();
            _mouseP = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)
            dragged = graph.nearest(_mouseP);

            if (dragged && dragged.node !== null){
              // while we're dragging, don't let physics move the node
              dragged.node.fixed = true

              renderer.selected_node = dragged.node.name;
              if(node_handler) {
                node_handler(dragged.node);
              }
            }

            $(canvas).bind('mousemove', handler.dragged)
            $(window).bind('mouseup', handler.dropped)

            return false
          },

          dragged: function(e){
            var pos = $(canvas).offset();
            var s = arbor.Point(e.pageX-pos.left, e.pageY-pos.top)

            if (dragged && dragged.node !== null){
              var p = graph.fromScreen(s)
              dragged.node.p = p
            }

            return false
          },

          dropped: function(e){
            if (dragged===null || dragged.node===undefined) return
            if (dragged.node !== null) dragged.node.fixed = false
            dragged.node.tempMass = 1000

            dragged = null
            $(canvas).unbind('mousemove', handler.dragged)
            $(window).unbind('mouseup', handler.dropped)
            _mouseP = null
            return false
          }
        }
        
        // start listening
        $(canvas).mousedown(handler.clicked);
      },
    }

    return renderer;
  }    

	var setup_graph = function() {
    var model = arbor.ParticleSystem({repulsion: 800, stiffness: 600, friction: 0.7, fps: 2});
    model.parameters({gravity:true}); // center-gravity to make the graph settle
    model.renderer = graph_renderer("#graph-view");

    /*
    model.addEdge('a','b');
    model.addEdge('a','c');
    model.addEdge('a','d');
    model.addEdge('a','e');
    model.addNode('f', {alone:true, mass:.25});
    */

    // or, equivalently:
    //
    // model.graft({
    //   nodes:{
    //     f:{alone:true, mass:.25}
    //   }, 
    //   edges:{
    //     a:{ b:{},
    //         c:{},
    //         d:{},
    //         e:{}
    //     }
    //   }
    // })
		return model;
  }

	var plasma_client = function(url, graph_model) {
		var socket;
    var handlers = {};

		if(window.WebSocket) {
			socket = new WebSocket(url);
			socket.onopen = function(event) { 
				$("#status").html("Connected to Plasma server");
			};

			socket.onclose = function(event) { 
				$("#status").html("Connection Closed");
			};

			socket.onmessage = function(event) { 
        var res = JSON.parse(event.data);
				//$("#footer").prepend("<div>" + event.data + "</div>");
        if(handlers[res.id]) {
          handlers[res.id](res.result);
          delete handlers[res.id];
        }
			};
		} 
		else {
			alert('Your browser does not support WebSockets yet.');
		}

		$(window).unload(function() {
			socket.close();
		});

		var client = {
			send: function(msg) {
				if (!window.WebSocket) { 
					return; 
				}

				if(socket.readyState) {
					socket.send(msg);
					console.log("sent: " + msg)
				} 
				else {
					$("#status").html("Connection Closed");
				}
      },

      request: function(method, params, handler) {
        var id = uuid();
        this.send(
          JSON.stringify({
            "method": method,
            "params": params,
            "id": id}));

        if (handler) {
          handlers[id] = handler;
        }

        return id;
      },

      query: function(query_txt, handler) {
        this.request("query", [query_txt], function(result) {
          console.log("query result: ");
          console.log(result);
          handler(result)
        });
      },

      graph: graph_model,

      clear_graph: function() {
        var g = this.graph;
        g.eachNode(function(node, pt) {
          g.pruneNode(node);
        });
      },

      add_node: function(id, node) {
        if(!node) {
          node = {id: id, edges: []};
        }

        this.graph.addNode(id, node);
        this.add_node_edges(node);
      },

      find_node: function(id, handler) {
        this.query("(find-node \"" + id + "\")", handler);
      },

      add_node_edges: function(node) {
        if(node.edges) {
          $.each(node.edges, function(tgt_id, props) {
            console.log("[add_node_edges] edge - from: " + node.id + " -> " + tgt_id);
            plasma.graph.addEdge(node.id, tgt_id, props);
          });
        }
      },

      expand_node: function(id, handler) {
        console.log("expanding node: " + id);
        this.find_node(id, function(node) {
          //jQuery.each(node.edges, function(prop, val) {
          //  console.log("[expand_node] edge - " + prop + ": " + val);
          //});
          if(node) {
            plasma.add_node(node.id, node);

            if(handler) {
              handler(node);
            }
          }
        });
      },

      show_node: function(n) {
        if(is_uuid(n)) {
          this.find_node(n, function(node) {
            plasma.show_object(node);
          });
        } else {
          plasma.show_object(n);
        }
      },

      show_object: function(obj) {
        $("#property-pane").html(prettyPrint(obj));
      },

      smart_query: function(query_txt) {
        console.log("smart_query: " + query_txt);
        plasma.query(query_txt, function(result) {
          console.log("smart_query result: " + result);
          plasma.show_object(result);

          if(is_uuid(result)) {
            plasma.clear_graph();
            plasma.add_node(result);
            plasma.show_node(result);
            plasma.expand_node(result);
          } else if (result.type == "node") {
            var node = result;
            plasma.clear_graph();
            plasma.add_node(node.id, node);
            plasma.show_node(result);
            plasma.expand_node(result);
          }
        });
      },
    };

    graph_model.renderer.on_node_clicked( function(node) {
      console.log("clicked: " + node.name);
      plasma.show_object(node);
      if(is_uuid(node.name)) {
        plasma.expand_node(node.name, plasma.show_node);
      }
    });

		return client;
	}

	var setup_query_view = function(plasma) {
    $("#run-btn").click(function() {
      var query_txt = $("#query-text").val();
      plasma.smart_query(query_txt);

      return false;
    });
	}

	var init = function() {
		graph_model = setup_graph();
		plasma = plasma_client('ws://localhost:4242/', graph_model);
		setup_query_view(plasma);
    $("#query-text").val("(root-node)");
    //window.setTimeout(function() { plasma.smart_query("(root-node)\n"); }, 500);
	}

  $(document).ready(init);

})(this.jQuery)

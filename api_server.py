from flask import Flask, request, jsonify
import uuid
import time
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('api_server')

app = Flask(__name__)

class Node:
    def __init__(self, cpu_cores, node_id=None):
        self.node_id = node_id if node_id else str(uuid.uuid4())
        self.cpu_cores = cpu_cores
        self.available_cores = cpu_cores
        self.pods = []  # List to store pod IDs
        self.status = "Ready"
        self.last_heartbeat = time.time()
        self.container_id = None  # Will store Docker container ID

    def to_dict(self):
        return {
            "node_id": self.node_id,
            "cpu_cores": self.cpu_cores,
            "available_cores": self.available_cores,
            "pods": self.pods,
            "status": self.status,
            "last_heartbeat": self.last_heartbeat,
            "container_id": self.container_id
        }

class APIServer:
    """Central control unit that manages the cluster operation"""
    
    def __init__(self):
        self.nodes = {}  # Dictionary to store nodes by node_id
        self.pods = {}   # Dictionary to store pods by pod_id
        self.heartbeat_timeout = 30  # Seconds before a node is considered failed
        
        # Start health monitoring thread
        self.health_monitor_thread = threading.Thread(target=self._monitor_node_health)
        self.health_monitor_thread.daemon = True
        self.health_monitor_thread.start()
        
        logger.info("API Server initialized")
    
    def register_node(self, cpu_cores, container_id=None):
        """Register a new node with the cluster"""
        new_node = Node(cpu_cores)
        new_node.container_id = container_id
        self.nodes[new_node.node_id] = new_node
        logger.info(f"Registered new node with ID: {new_node.node_id}, CPU Cores: {cpu_cores}")
        return new_node
    
    def get_node(self, node_id):
        """Get a node by ID"""
        return self.nodes.get(node_id)
    
    def list_nodes(self):
        """Return list of all nodes and their details"""
        return [node.to_dict() for node in self.nodes.values()]
    
    def update_node_heartbeat(self, node_id):
        """Update the last heartbeat time for a node"""
        if node_id in self.nodes:
            self.nodes[node_id].last_heartbeat = time.time()
            return True
        return False
    
    def _monitor_node_health(self):
        """Background thread to monitor node health based on heartbeats"""
        while True:
            current_time = time.time()
            for node_id, node in list(self.nodes.items()):
                # If node hasn't sent heartbeat within timeout period
                if current_time - node.last_heartbeat > self.heartbeat_timeout:
                    if node.status == "Ready":
                        logger.warning(f"Node {node_id} not responding, marking as NotReady")
                        node.status = "NotReady"
                    elif node.status == "NotReady":
                        # After additional timeout, mark as Failed
                        # In future weeks, we'll implement pod rescheduling here
                        logger.error(f"Node {node_id} failed")
                        node.status = "Failed"
            
            # Check every 5 seconds
            time.sleep(5)

# Create instance of the API Server
api_server = APIServer()

@app.route('/nodes', methods=['POST'])
def add_node():
    """API endpoint to add a new node to the cluster"""
    data = request.json
    
    if not data or 'cpu_cores' not in data:
        return jsonify({"error": "CPU cores specification required"}), 400
    
    try:
        cpu_cores = int(data['cpu_cores'])
        if cpu_cores <= 0:
            return jsonify({"error": "CPU cores must be positive"}), 400
            
        container_id = data.get('container_id')
        new_node = api_server.register_node(cpu_cores, container_id)
        
        return jsonify({
            "message": "Node added successfully",
            "node_id": new_node.node_id,
            "cpu_cores": new_node.cpu_cores
        }), 201
        
    except ValueError:
        return jsonify({"error": "CPU cores must be a number"}), 400

@app.route('/nodes', methods=['GET'])
def list_nodes():
    """API endpoint to list all nodes in the cluster"""
    nodes = api_server.list_nodes()
    return jsonify({"nodes": nodes, "count": len(nodes)}), 200

@app.route('/nodes/<node_id>/heartbeat', methods=['POST'])
def update_heartbeat(node_id):
    """API endpoint for nodes to send heartbeat signals"""
    success = api_server.update_node_heartbeat(node_id)
    if success:
        return jsonify({"status": "Heartbeat received"}), 200
    else:
        return jsonify({"error": "Node not found"}), 404

@app.route('/health', methods=['GET'])
def health_check():
    """Basic health check endpoint for the API server itself"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
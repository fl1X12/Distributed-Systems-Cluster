import docker
import logging
import requests
import time
import os
import signal
import threading
import sys
import pickle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('node_container')

class NodeContainer:
    """Class to simulate a node in the cluster using Docker containers"""
    
    def __init__(self, cpu_cores, api_server_url):
        self.cpu_cores = cpu_cores
        self.api_server_url = api_server_url
        self.identifier=None # short identifier to fetch node
        self.node_id = None
        self.docker_client = None  # Will be initialized on demand
        self.container_id = None
        self.heartbeat_thread = None
        self.running = False
    
    def _get_docker_client(self):
        """Get or create a Docker client with appropriate configuration for the platform"""
        if self.docker_client is None:
            try:
                # Try different Docker connection options based on the platform
                if os.name == 'nt':  # Windows
                    self.docker_client = docker.DockerClient(base_url='npipe:////./pipe/docker_engine')
                else:  # Linux/Mac
                    self.docker_client = docker.from_env()
            except Exception as e:
                logger.error(f"Failed to initialize Docker client: {e}")
                # Fall back to simulated mode
                self.docker_client = SimulatedDockerClient()
                logger.warning("Using simulated Docker client due to connection error")
        
        return self.docker_client
    
    def start(self):
        """Launch the container and register with API server"""
        try:
            docker_client = self._get_docker_client()
            
            # Check if this is a simulated client
            is_simulated = isinstance(docker_client, SimulatedDockerClient)
            
            if not is_simulated:
                # Launch Docker container for the node
                try:
                    container = docker_client.containers.run(
                        "python:3.9-slim",  # Using a minimal Python image
                        command="tail -f /dev/null",  # Keep container running
                        detach=True,
                        name=f"kubernetes_sim_node_{int(time.time())}",
                        labels={"app": "kubernetes_sim", "type": "node"}
                    )
                    self.container_id = container.id
                    logger.info(f"Started container: {self.container_id}")
                except Exception as e:
                    logger.error(f"Error launching container: {e}")
                    # Fall back to simulated mode
                    is_simulated = True
            
            if is_simulated:
                # Create a simulated container ID
                self.container_id = f"simulated_{int(time.time())}"
                logger.warning(f"Using simulated container with ID: {self.container_id}")
            
            # Register with API server
            response = requests.post(
                f"{self.api_server_url}/nodes",
                json={"cpu_cores": self.cpu_cores, "container_id": self.container_id}
            )
            
            if response.status_code == 201:
                self.node_id = response.json()["node_id"]
                logger.info(f"Registered with API server, node ID: {self.node_id}")
                
                # Start heartbeat thread
                self.running = True
                self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
                self.heartbeat_thread.daemon = True
                self.heartbeat_thread.start()
                
                return True
            else:
                logger.error(f"Failed to register with API server: {response.text}")
                self._cleanup_container()
                return False
                
        except Exception as e:
            logger.error(f"Error starting node container: {e}")
            self._cleanup_container()
            return False
    
    def _cleanup_container(self):
        """Clean up the container if needed"""
        if self.container_id and not isinstance(self._get_docker_client(), SimulatedDockerClient):
            try:
                container = self._get_docker_client().containers.get(self.container_id)
                container.stop()
                container.remove()
                logger.info(f"Cleaned up container {self.container_id}")
            except Exception as e:
                logger.error(f"Error cleaning up container: {e}")
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to the API server"""
        while self.running:
            try:
                response = requests.post(f"{self.api_server_url}/nodes/{self.node_id}/heartbeat")
                if response.status_code == 200:
                    logger.debug(f"Heartbeat sent for node {self.node_id}")
                else:
                    logger.warning(f"Failed to send heartbeat: {response.text}")
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
            
            # Sleep for 10 seconds before next heartbeat
            time.sleep(10)
    
    def stop(self):
        """Stop the node container"""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(1)
        
        self._cleanup_container()
        logger.info(f"Node {self.node_id} stopped")

    def __getstate__(self):
        """Prepare object for pickling (serialization)"""
        state = self.__dict__.copy()
        # Don't pickle the Docker client or thread
        state['docker_client'] = None
        state['heartbeat_thread'] = None
        return state
    
    def __setstate__(self, state):
        """Restore object after unpickling (deserialization)"""
        self.__dict__.update(state)
        # Restart the heartbeat thread if the node was running
        if self.running:
            self.heartbeat_thread = threading.Thread(target=self._send_heartbeats)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()


class SimulatedDockerClient:
    """A simulated Docker client for environments where Docker is not available"""
    
    class Container:
        def __init__(self, id):
            self.id = id
        
        def stop(self):
            pass
        
        def remove(self):
            pass
    
    class ContainerCollection:
        def run(self, image, **kwargs):
            container_id = f"simulated_{int(time.time())}"
            return SimulatedDockerClient.Container(container_id)
        
        def get(self, container_id):
            return SimulatedDockerClient.Container(container_id)
    
    def __init__(self):
        self.containers = self.ContainerCollection()


def add_node(cpu_cores, api_server_url):
    """Helper function to add a new node to the cluster"""
    node = NodeContainer(cpu_cores, api_server_url)
    success = node.start()
    if success:
        return node
    return None


if __name__ == "__main__":
    # Simple CLI for testing node creation
    api_url = os.environ.get("API_SERVER_URL", "http://localhost:5000")
    
    if len(sys.argv) < 2:
        print("Usage: python node_container.py <cpu_cores>")
        sys.exit(1)
    
    try:
        cores = int(sys.argv[1])
        node = add_node(cores, api_url)
        
        if node:
            print(f"Node added with ID: {node.node_id}")
            
            # Keep running until interrupted
            def signal_handler(sig, frame):
                print("Stopping node...")
                node.stop()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            print("Press Ctrl+C to stop the node")
            signal.pause()
        else:
            print("Failed to add node")
            sys.exit(1)
    except ValueError:
        print("CPU cores must be a number")
        sys.exit(1)
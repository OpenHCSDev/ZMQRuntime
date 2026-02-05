"""
ZMQ configuration for viewer servers.
Extracts OpenHCS-specific ZMQ configuration to be reusable.
"""

from typing import Optional

from zmqruntime.config import ZMQConfig, TransportMode


class ViewerZMQConfig:
    """
    ZMQ configuration for viewer servers.
    
    Extracts OpenHCS-specific ZMQ configuration to be reusable.
    """
    
    def __init__(
        self,
        zmq_config: Optional[ZMQConfig] = None,
        app_name: str = "openhcs",
    ):
        """
        Initialize viewer ZMQ config.
        
        Args:
            zmq_config: Generic ZMQConfig, or None
            app_name: App name for socket paths
        """
        if zmq_config is None:
            # Create default config with OpenHCS app paths
            from pathlib import Path
            
            app_dir = Path.home() / f".{app_name}"
            config = ZMQConfig(
                control_port_offset=1000,
                default_port=7777,
                ipc_socket_dir=app_dir / "ipc",
                ipc_socket_prefix=f"{app_name}-zmq",
                ipc_socket_extension=".sock",
                shared_ack_port=7555,
            )
        else:
            # Use provided config
            config = zmq_config
        
        self._config = config
    
    @property
    def config(self) -> ZMQConfig:
        """Get ZMQConfig instance."""
        return self._config

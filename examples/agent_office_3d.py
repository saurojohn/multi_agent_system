"""3D AI Agent Office Dashboard - WebGL powered 3D office visualization."""

import time
import sys
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import random

this_dir = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.abspath(os.path.join(this_dir, '..', 'src'))
sys.path.insert(0, _SRC_DIR)

from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator
from multi_agent_system.worker.agent import WorkerAgent


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html = self.get_dashboard_html()
            self.wfile.write(html.encode())
        elif self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            status = {
                'workers': self.server.orch.get_workers_status(),
                'tasks': {tid: {
                    'status': t.status,
                    'task_type': t.task_type,
                    'task_data': t.task_data,
                    'result': t.result,
                    'error': t.error
                } for tid, t in self.server.orch.tasks.items()}
            }
            self.wfile.write(json.dumps(status).encode())
        else:
            super().do_GET()

    def get_dashboard_html(self):
        return '''<!DOCTYPE html>
<html>
<head>
    <title>3D AI Agent Office</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            background: #0a0a15;
            overflow: hidden;
            color: #fff;
        }
        #canvas-container {
            width: 100vw;
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
        }
        #ui-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            z-index: 100;
        }
        .top-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 30px;
            background: linear-gradient(180deg, rgba(0,0,0,0.7) 0%, transparent 100%);
        }
        .title {
            font-size: 24px;
            font-weight: bold;
            color: #00d9ff;
            text-shadow: 0 0 20px rgba(0,217,255,0.5);
        }
        .title span { font-size: 28px; margin-right: 10px; }
        .clock {
            font-size: 28px;
            font-family: 'Courier New', monospace;
            color: #00ff88;
            text-shadow: 0 0 15px rgba(0,255,136,0.5);
        }
        .info-panel {
            position: fixed;
            bottom: 20px;
            left: 20px;
            background: rgba(0,20,40,0.85);
            border: 1px solid rgba(0,217,255,0.3);
            border-radius: 15px;
            padding: 20px;
            min-width: 300px;
            backdrop-filter: blur(10px);
        }
        .info-panel h3 {
            color: #00d9ff;
            margin-bottom: 15px;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 2px;
        }
        .stat-row {
            display: flex;
            justify-content: space-between;
            margin: 8px 0;
            font-size: 14px;
        }
        .stat-label { color: #888; }
        .stat-value { color: #fff; font-weight: bold; }
        .task-panel {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: rgba(0,20,40,0.85);
            border: 1px solid rgba(0,217,255,0.3);
            border-radius: 15px;
            padding: 20px;
            width: 350px;
            max-height: 400px;
            overflow-y: auto;
            backdrop-filter: blur(10px);
        }
        .task-panel h3 {
            color: #00d9ff;
            margin-bottom: 15px;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 2px;
        }
        .task-item {
            background: rgba(0,0,0,0.3);
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 10px;
            border-left: 3px solid;
        }
        .task-item.completed { border-color: #00ff88; }
        .task-item.failed { border-color: #ff4757; }
        .task-item.pending { border-color: #ffa502; }
        .task-item.running { border-color: #00d9ff; }
        .task-type {
            font-size: 11px;
            color: #00d9ff;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        .task-id { font-size: 12px; color: #666; font-family: monospace; }
        .task-result {
            font-size: 11px;
            color: #00ff88;
            margin-top: 5px;
            word-break: break-all;
        }
        .controls {
            position: fixed;
            top: 80px;
            right: 20px;
            background: rgba(0,20,40,0.85);
            border: 1px solid rgba(0,217,255,0.3);
            border-radius: 15px;
            padding: 15px;
            backdrop-filter: blur(10px);
        }
        .controls h4 {
            color: #888;
            font-size: 11px;
            text-transform: uppercase;
            margin-bottom: 10px;
        }
        .control-btn {
            display: block;
            width: 100%;
            padding: 8px 15px;
            margin: 5px 0;
            background: rgba(0,217,255,0.2);
            border: 1px solid rgba(0,217,255,0.5);
            border-radius: 8px;
            color: #00d9ff;
            cursor: pointer;
            pointer-events: auto;
            font-size: 12px;
            transition: all 0.3s;
        }
        .control-btn:hover {
            background: rgba(0,217,255,0.4);
        }
        .tooltip {
            position: fixed;
            background: rgba(0,0,0,0.9);
            border: 1px solid #00d9ff;
            border-radius: 10px;
            padding: 15px;
            pointer-events: none;
            display: none;
            z-index: 200;
            min-width: 200px;
        }
        .tooltip h4 {
            color: #00d9ff;
            margin-bottom: 10px;
        }
        .tooltip p {
            font-size: 12px;
            margin: 5px 0;
        }
        .tooltip .status-badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 10px;
            font-size: 10px;
            margin-left: 10px;
        }
        .status-badge.online { background: #00ff8833; color: #00ff88; }
        .status-badge.offline { background: #ff475733; color: #ff4757; }
        .status-badge.busy { background: #ffa50233; color: #ffa502; }
        .status-badge.idle { background: #00d9ff33; color: #00d9ff; }
    </style>
</head>
<body>
    <div id="canvas-container"></div>
    <div id="ui-overlay">
        <div class="top-bar">
            <div class="title"><span>🏢</span> 3D AI Agent Office</div>
            <div class="clock" id="clock">--:--:--</div>
        </div>

        <div class="info-panel">
            <h3>📊 Office Statistics</h3>
            <div class="stat-row">
                <span class="stat-label">Total Agents</span>
                <span class="stat-value" id="stat-total">0</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Online</span>
                <span class="stat-value" id="stat-online">0</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Busy</span>
                <span class="stat-value" id="stat-busy">0</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Tasks Completed</span>
                <span class="stat-value" id="stat-completed">0</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Tasks Pending</span>
                <span class="stat-value" id="stat-pending">0</span>
            </div>
        </div>

        <div class="controls">
            <h4>Camera Views</h4>
            <button class="control-btn" onclick="cameraView('front')">Front View</button>
            <button class="control-btn" onclick="cameraView('top')">Top View</button>
            <button class="control-btn" onclick="cameraView('corner')">Corner View</button>
            <button class="control-btn" onclick="toggleRotation()">Auto Rotate</button>
        </div>

        <div class="task-panel">
            <h3>📋 Task Board</h3>
            <div id="task-list">No tasks yet...</div>
        </div>
    </div>

    <div class="tooltip" id="tooltip">
        <h4 id="tooltip-name">Agent Name</h4>
        <p>Type: <span id="tooltip-type">-</span></p>
        <p>Status: <span id="tooltip-status" class="status-badge">-</span></p>
        <p>Completed: <span id="tooltip-completed">0</span></p>
        <p>Failed: <span id="tooltip-failed">0</span></p>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script>
        // Scene setup
        const container = document.getElementById('canvas-container');
        const scene = new THREE.Scene();
        scene.background = new THREE.Color(0x0a0a15);
        scene.fog = new THREE.Fog(0x0a0a15, 50, 150);

        const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 1000);
        camera.position.set(40, 30, 40);
        camera.lookAt(0, 0, 0);

        const renderer = new THREE.WebGLRenderer({ antialias: true });
        renderer.setSize(window.innerWidth, window.innerHeight);
        renderer.shadowMap.enabled = true;
        renderer.shadowMap.type = THREE.PCFSoftShadowMap;
        container.appendChild(renderer.domElement);

        // Lighting
        const ambientLight = new THREE.AmbientLight(0x404080, 0.5);
        scene.add(ambientLight);

        const mainLight = new THREE.DirectionalLight(0xffffff, 1);
        mainLight.position.set(30, 50, 30);
        mainLight.castShadow = true;
        mainLight.shadow.mapSize.width = 2048;
        mainLight.shadow.mapSize.height = 2048;
        mainLight.shadow.camera.near = 0.5;
        mainLight.shadow.camera.far = 200;
        mainLight.shadow.camera.left = -50;
        mainLight.shadow.camera.right = 50;
        mainLight.shadow.camera.top = 50;
        mainLight.shadow.camera.bottom = -50;
        scene.add(mainLight);

        const fillLight = new THREE.DirectionalLight(0x00d9ff, 0.3);
        fillLight.position.set(-20, 20, -20);
        scene.add(fillLight);

        // Floor
        const floorGeometry = new THREE.PlaneGeometry(100, 100);
        const floorMaterial = new THREE.MeshStandardMaterial({
            color: 0x1a1a2e,
            roughness: 0.8,
            metalness: 0.2
        });
        const floor = new THREE.Mesh(floorGeometry, floorMaterial);
        floor.rotation.x = -Math.PI / 2;
        floor.receiveShadow = true;
        scene.add(floor);

        // Grid
        const gridHelper = new THREE.GridHelper(100, 50, 0x00d9ff, 0x1a1a3e);
        gridHelper.position.y = 0.01;
        scene.add(gridHelper);

        // Office walls
        function createWall(x, z, rotationY, width, height) {
            const geometry = new THREE.BoxGeometry(width, height, 0.5);
            const material = new THREE.MeshStandardMaterial({
                color: 0x16213e,
                roughness: 0.5,
                metalness: 0.3,
                transparent: true,
                opacity: 0.7
            });
            const wall = new THREE.Mesh(geometry, material);
            wall.position.set(x, height / 2, z);
            wall.rotation.y = rotationY;
            wall.castShadow = true;
            wall.receiveShadow = true;
            return wall;
        }

        scene.add(createWall(-25, 0, 0, 0.5, 15));
        scene.add(createWall(25, 0, 0, 0.5, 15));
        scene.add(createWall(0, -25, Math.PI / 2, 50, 15));

        // Desk function
        function createDesk(x, z, color) {
            const deskGroup = new THREE.Group();

            // Desk top
            const topGeo = new THREE.BoxGeometry(4, 0.2, 2);
            const topMat = new THREE.MeshStandardMaterial({ color: 0x2d4a3e, roughness: 0.3 });
            const top = new THREE.Mesh(topGeo, topMat);
            top.position.y = 2.5;
            top.castShadow = true;
            top.receiveShadow = true;
            deskGroup.add(top);

            // Legs
            const legGeo = new THREE.CylinderGeometry(0.1, 0.1, 2.5);
            const legMat = new THREE.MeshStandardMaterial({ color: 0x333333 });
            const positions = [[-1.7, -0.8], [1.7, -0.8], [-1.7, 0.8], [1.7, 0.8]];
            positions.forEach(([lx, lz]) => {
                const leg = new THREE.Mesh(legGeo, legMat);
                leg.position.set(lx, 1.25, lz);
                leg.castShadow = true;
                deskGroup.add(leg);
            });

            // Monitor
            const monGeo = new THREE.BoxGeometry(1.5, 1, 0.1);
            const monMat = new THREE.MeshStandardMaterial({ color: 0x111111 });
            const monitor = new THREE.Mesh(monGeo, monMat);
            monitor.position.set(0, 3.3, -0.5);
            monitor.castShadow = true;
            deskGroup.add(monitor);

            // Screen glow
            const screenGeo = new THREE.PlaneGeometry(1.3, 0.8);
            const screenMat = new THREE.MeshBasicMaterial({ color: color, transparent: true, opacity: 0.8 });
            const screen = new THREE.Mesh(screenGeo, screenMat);
            screen.position.set(0, 3.3, -0.44);
            deskGroup.add(screen);

            // Chair
            const chairGeo = new THREE.CylinderGeometry(0.4, 0.4, 0.3);
            const chairMat = new THREE.MeshStandardMaterial({ color: 0x1a1a1a });
            const chair = new THREE.Mesh(chairGeo, chairMat);
            chair.position.set(0, 0.5, 2);
            chair.castShadow = true;
            deskGroup.add(chair);

            deskGroup.position.set(x, 0, z);
            return deskGroup;
        }

        // Agent robot function
        function createAgent(type, color) {
            const agentGroup = new THREE.Group();

            // Body
            const bodyGeo = new THREE.BoxGeometry(1.2, 1.5, 0.8);
            const bodyMat = new THREE.MeshStandardMaterial({ color: color, roughness: 0.3, metalness: 0.7 });
            const body = new THREE.Mesh(bodyGeo, bodyMat);
            body.position.y = 2.5;
            body.castShadow = true;
            agentGroup.add(body);

            // Head
            const headGeo = new THREE.BoxGeometry(0.8, 0.8, 0.8);
            const headMat = new THREE.MeshStandardMaterial({ color: 0xdddddd, roughness: 0.3, metalness: 0.9 });
            const head = new THREE.Mesh(headGeo, headMat);
            head.position.y = 3.6;
            head.castShadow = true;
            agentGroup.add(head);

            // Eyes
            const eyeGeo = new THREE.SphereGeometry(0.1);
            const eyeMat = new THREE.MeshBasicMaterial({ color: 0x00ff88 });
            const leftEye = new THREE.Mesh(eyeGeo, eyeMat);
            leftEye.position.set(-0.2, 3.7, 0.4);
            agentGroup.add(leftEye);
            const rightEye = new THREE.Mesh(eyeGeo, eyeMat);
            rightEye.position.set(0.2, 3.7, 0.4);
            agentGroup.add(rightEye);

            // Antenna
            const antGeo = new THREE.CylinderGeometry(0.05, 0.05, 0.5);
            const antMat = new THREE.MeshStandardMaterial({ color: 0x888888 });
            const antenna = new THREE.Mesh(antGeo, antMat);
            antenna.position.y = 4.2;
            agentGroup.add(antenna);

            const antBallGeo = new THREE.SphereGeometry(0.1);
            const antBallMat = new THREE.MeshBasicMaterial({ color: color });
            const antBall = new THREE.Mesh(antBallGeo, antBallMat);
            antBall.position.y = 4.5;
            agentGroup.add(antBall);

            // Status light
            const statusGeo = new THREE.SphereGeometry(0.15);
            const statusMat = new THREE.MeshBasicMaterial({ color: 0x00ff88 });
            const statusLight = new THREE.Mesh(statusGeo, statusMat);
            statusLight.position.set(0, 2.2, 0.5);
            agentGroup.add(statusLight);

            // Type label (3D text using a plane with canvas texture)
            const canvas = document.createElement('canvas');
            canvas.width = 256;
            canvas.height = 64;
            const ctx = canvas.getContext('2d');
            ctx.fillStyle = 'transparent';
            ctx.fillRect(0, 0, 256, 64);
            ctx.fillStyle = '#00d9ff';
            ctx.font = 'bold 24px Arial';
            ctx.textAlign = 'center';
            ctx.fillText(type.toUpperCase(), 128, 40);

            const labelTex = new THREE.CanvasTexture(canvas);
            const labelMat = new THREE.MeshBasicMaterial({ map: labelTex, transparent: true });
            const labelGeo = new THREE.PlaneGeometry(2, 0.5);
            const label = new THREE.Mesh(labelGeo, labelMat);
            label.position.set(0, 4.8, 0);
            agentGroup.add(label);

            agentGroup.userData = { statusLight, statusMat, type, color };
            return agentGroup;
        }

        // Create office layout
        const desks = [];
        const agents = [];
        const agentColors = [0x00d9ff, 0xff6b6b, 0x4ecdc4, 0xffa502, 0x9b59b6];
        const agentTypes = ['ANALYSIS', 'RESEARCH', 'CODING', 'DESIGN', 'DATA'];

        // Row 1
        for (let i = 0; i < 3; i++) {
            const desk = createDesk(-15 + i * 10, -10, agentColors[i]);
            scene.add(desk);
            desks.push(desk);
        }

        // Row 2
        for (let i = 0; i < 2; i++) {
            const desk = createDesk(-10 + i * 15, -20, agentColors[i + 3]);
            scene.add(desk);
            desks.push(desk);
        }

        // Create agents (5 agents)
        const agentPositions = [
            { x: -15, z: -10 }, { x: -5, z: -10 }, { x: 5, z: -10 },
            { x: -10, z: -20 }, { x: 5, z: -20 }
        ];

        for (let i = 0; i < 5; i++) {
            const agent = createAgent(agentTypes[i], agentColors[i]);
            agent.position.set(agentPositions[i].x, 0, agentPositions[i].z);
            scene.add(agent);
            agents.push(agent);
        }

        // Animation state
        let autoRotate = true;
        let time = 0;

        // Camera views
        const views = {
            front: { x: 0, y: 20, z: 50 },
            top: { x: 0, y: 60, z: 0.1 },
            corner: { x: 40, y: 30, z: 40 }
        };

        window.cameraView = function(name) {
            const v = views[name];
            camera.position.set(v.x, v.y, v.z);
            camera.lookAt(0, 0, 0);
        };

        window.toggleRotation = function() {
            autoRotate = !autoRotate;
        };

        // Update agents based on real data
        function updateAgents(workers) {
            workers.forEach((w, i) => {
                if (i < agents.length) {
                    const agent = agents[i];
                    const { statusLight, statusMat } = agent.userData;

                    // Update status light color
                    if (w.status === 'online') {
                        statusMat.color.setHex(0x00ff88);
                    } else if (w.status === 'busy') {
                        statusMat.color.setHex(0xffa502);
                    } else {
                        statusMat.color.setHex(0xff4757);
                    }

                    // Gentle bobbing animation
                    agent.position.y = Math.sin(time * 2 + i) * 0.1;
                }
            });
        }

        // Raycaster for hover
        const raycaster = new THREE.Raycaster();
        const mouse = new THREE.Vector2();
        const tooltip = document.getElementById('tooltip');

        function onMouseMove(event) {
            mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
            mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;

            raycaster.setFromCamera(mouse, camera);
            const intersects = raycaster.intersectObjects(agents, true);

            if (intersects.length > 0) {
                let obj = intersects[0].object;
                while (obj.parent && !obj.userData.type) {
                    obj = obj.parent;
                }
                if (obj.userData.type) {
                    tooltip.style.display = 'block';
                    tooltip.style.left = event.clientX + 15 + 'px';
                    tooltip.style.top = event.clientY + 15 + 'px';

                    // Find matching worker data
                    const idx = agents.indexOf(obj);
                    if (idx >= 0 && window.workerData && window.workerData[idx]) {
                        const w = window.workerData[idx];
                        document.getElementById('tooltip-name').textContent = w.worker_id;
                        document.getElementById('tooltip-type').textContent = w.worker_type;
                        document.getElementById('tooltip-status').textContent = w.status.toUpperCase();
                        document.getElementById('tooltip-status').className = 'status-badge ' + w.status;
                        document.getElementById('tooltip-completed').textContent = w.completed;
                        document.getElementById('tooltip-failed').textContent = w.failed;
                    }
                }
            } else {
                tooltip.style.display = 'none';
            }
        }

        document.addEventListener('mousemove', onMouseMove);

        // Resize handler
        window.addEventListener('resize', () => {
            camera.aspect = window.innerWidth / window.innerHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(window.innerWidth, window.innerHeight);
        });

        // Clock
        function updateClock() {
            const now = new Date();
            document.getElementById('clock').textContent = now.toLocaleTimeString();
        }
        setInterval(updateClock, 1000);
        updateClock();

        // API data fetch and UI update
        async function updateUI() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                window.workerData = data.workers;

                // Update stats
                const total = data.workers.length;
                const online = data.workers.filter(w => w.status === 'online' || w.status === 'busy').length;
                const busy = data.workers.filter(w => w.status === 'busy').length;
                const completed = data.workers.reduce((sum, w) => sum + w.completed, 0);
                const pending = Object.values(data.tasks).filter(t => t.status === 'pending' || t.status === 'running').length;

                document.getElementById('stat-total').textContent = total;
                document.getElementById('stat-online').textContent = online;
                document.getElementById('stat-busy').textContent = busy;
                document.getElementById('stat-completed').textContent = completed;
                document.getElementById('stat-pending').textContent = pending;

                // Update agents
                updateAgents(data.workers);

                // Update task list
                let taskHtml = '';
                const tasks = Object.entries(data.tasks).slice(-8).reverse();
                if (tasks.length === 0) {
                    taskHtml = '<p style="color:#666;text-align:center;">No tasks yet...</p>';
                }
                tasks.forEach(([tid, t]) => {
                    taskHtml += `<div class="task-item ${t.status}">
                        <div class="task-type">${t.task_type}</div>
                        <div class="task-id">#${tid.substring(0, 8)}</div>
                        ${t.result ? `<div class="task-result">✓ ${JSON.stringify(t.result)}</div>` : ''}
                        ${t.error ? `<div class="task-result" style="color:#ff4757;">✗ ${t.error}</div>` : ''}
                    </div>`;
                });
                document.getElementById('task-list').innerHTML = taskHtml;
            } catch (e) {
                console.error(e);
            }
        }

        setInterval(updateUI, 1000);
        updateUI();

        // Animation loop
        function animate() {
            requestAnimationFrame(animate);
            time += 0.016;

            if (autoRotate) {
                camera.position.x = Math.sin(time * 0.1) * 50;
                camera.position.z = Math.cos(time * 0.1) * 50;
                camera.position.y = 30 + Math.sin(time * 0.2) * 10;
                camera.lookAt(0, 0, 0);
            }

            // Animate agent antenna lights
            agents.forEach((agent, i) => {
                const light = agent.userData.statusLight;
                if (light) {
                    light.scale.setScalar(1 + Math.sin(time * 3 + i) * 0.2);
                }
            });

            renderer.render(scene, camera);
        }
        animate();
    </script>
</body>
</html>'''


def run_dashboard(orch, port=8080):
    server = HTTPServer(('localhost', port), DashboardHandler)
    server.orch = orch
    print(f'')
    print(f'  ╔═══════════════════════════════════════════╗')
    print(f'  ║   🏢 3D AI Agent Office Dashboard        ║')
    print(f'  ╠═══════════════════════════════════════════╣')
    print(f'  ║   Running at: http://localhost:{port}     ║')
    print(f'  ║   Press Ctrl+C to stop                  ║')
    print(f'  ╚═══════════════════════════════════════════╝')
    print(f'')
    server.serve_forever()


if __name__ == "__main__":
    mq = MessageQueueManager()
    orch = Orchestrator(mq)
    orch.start()

    # Create 5 workers
    workers = []
    worker_defs = [
        ("worker_1", "Analysis", ["analysis"]),
        ("worker_2", "Research", ["research"]),
        ("worker_3", "Coding", ["coding"]),
        ("worker_4", "Design", ["design"]),
        ("worker_5", "Data", ["data"]),
    ]

    def make_handler(task_type):
        def handler(task_data):
            query = task_data.get("task_data", {}).get("query", "")
            time.sleep(random.uniform(1, 2))
            return {task_type: f"Completed: {query}"}
        return handler

    for worker_id, worker_type, caps in worker_defs:
        w = WorkerAgent(worker_id, worker_type, caps, mq)
        w.register_handler(caps[0], make_handler(caps[0]))
        w.start()
        workers.append(w)

    time.sleep(2)

    # Submit sample tasks
    tasks = [
        ("analysis", {"query": "Q1 revenue trends"}),
        ("research", {"query": "market analysis"}),
        ("coding", {"query": "API integration"}),
        ("design", {"query": "UI mockups"}),
        ("data", {"query": "data pipeline"}),
        ("analysis", {"query": " competitor study"}),
        ("research", {"query": "tech trends"}),
        ("coding", {"query": "auth system"}),
    ]

    print("📋 Submitting tasks to the office...")
    for task_type, task_data in tasks:
        task_id = orch.submit_task(task_type, task_data)
        print(f"   ✓ {task_type}: {task_id[:8]}...")

    print("")
    print("🚀 Starting 3D dashboard...")
    print("")

    run_dashboard(orch)
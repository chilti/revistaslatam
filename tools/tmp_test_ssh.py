import paramiko
import os

hostname = 'dinamica1.fciencias.unam.mx'
username = 'jlja'
password = 'T3mporal123+-'
remote_path = '/mnt/expansion/openalex/openalex-snapshot/'

def test_ssh():
    print(f"Intentando conectar a {hostname}...")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, username=username, password=password)
        
        print("✅ Conexión SSH exitosa.")
        
        stdin, stdout, stderr = client.exec_command(f"ls -F {remote_path}")
        entities = stdout.read().decode().splitlines()
        
        print(f"Entidades encontradas en {remote_path}:")
        for e in entities:
            if e.endswith('/'):
                print(f"  - {e}")
        
        client.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_ssh()

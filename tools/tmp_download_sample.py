import paramiko
import os
from stat import S_ISDIR

hostname = 'dinamica1.fciencias.unam.mx'
username = 'jlja'
password = 'T3mporal123+-'
remote_base = '/mnt/expansion/openalex/openalex-snapshot/data/'
local_base = 'c:/Users/jlja/Documents/Proyectos/revistaslatam/test_snapshot/'

entities = ['sources', 'works']

def download_sample():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        sftp = ssh.open_sftp()
        
        for entity in entities:
            remote_entity_path = remote_base + entity
            local_entity_path = os.path.join(local_base, entity)
            os.makedirs(local_entity_path, exist_ok=True)
            
            print(f"Buscando archivos en {remote_entity_path}...")
            # Encontrar el primer subdirectorio (updated_date=...)
            subdirs = sftp.listdir(remote_entity_path)
            if not subdirs: continue
            
            remote_subpath = remote_entity_path + "/" + subdirs[0]
            files = [f for f in sftp.listdir(remote_subpath) if f.endswith('.gz')]
            
            # Descargar solo los primeros 2 archivos para prueba
            for f in files[:2]:
                remote_f = remote_subpath + "/" + f
                local_f = os.path.join(local_entity_path, f)
                print(f"  Descargando {f} -> {local_f}")
                sftp.get(remote_f, local_f)
                
        sftp.close()
        ssh.close()
        print("✅ Muestra descargada exitosamente.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    download_sample()

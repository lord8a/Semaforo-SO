import threading
import time
import random
import statistics
from collections import defaultdict

# --- Semáforos y mutex ---
mutex = threading.Lock()          # Protege el contador de lectores
read_try = threading.Semaphore(1) # Permite prioridad a escritores (evita inanición)
resource = threading.Semaphore(1) # Controla acceso a la BD (escritura exclusiva)

# --- Base de datos simulada (RAM) ---
database = {
    "S12345": 85,
    "S67890": 92,
    "S11111": 70,
}

readers_count = 0
active_writer = False

# --- Estadísticas para telemetría ---
wait_times_writers = []  # lista de tiempos de espera (segundos)
reader_log = defaultdict(int)  # para registrar carga durante escrituras

# --- Funciones del sistema ---
def read_database(student_id):
    """Simula lectura de nota (sin modificar datos)"""
    time.sleep(random.uniform(0.001, 0.005))  # simula tiempo de lectura
    return database.get(student_id, "No encontrado")

def write_database(student_id, new_grade):
    """Simula escritura en BD"""
    time.sleep(random.uniform(0.005, 0.01))   # simula tiempo de escritura
    database[student_id] = new_grade

def reader(reader_id, student_id):
    global readers_count

    # ---- Entrada con prioridad a escritores ----
    read_try.acquire()   # Si un escritor espera, este semáforo bloquea nuevos lectores
    mutex.acquire()
    readers_count += 1
    if readers_count == 1:
        resource.acquire()  # Primer lector bloquea escritores
    mutex.release()
    read_try.release()
    # -------------------------------------------

    # Lectura
    grade = read_database(student_id)
    print(f"📖 Lector {reader_id} leyendo nota de {student_id}: {grade}")

    # ---- Salida ----
    mutex.acquire()
    readers_count -= 1
    if readers_count == 0:
        resource.release()  # Último lector libera para escritores
    mutex.release()

def writer(writer_id, student_id, new_grade):
    global active_writer
    start_wait = time.time()

    # ---- Entrada con prioridad a escritores ----
    read_try.acquire()   # Previene nuevos lectores
    resource.acquire()   # Espera a que lectores actuales terminen
    # Después de este punto, escritor tiene acceso exclusivo

    wait_time = time.time() - start_wait
    wait_times_writers.append(wait_time)
    # Registrar cuántos lectores estaban activos cuando este escritor empezó a esperar
    reader_log[readers_count] += 1

    active_writer = True
    print(f"✍️  Escritor {writer_id} modificando nota de {student_id} a {new_grade} (esperó {wait_time:.4f}s)")
    write_database(student_id, new_grade)
    active_writer = False

    resource.release()
    read_try.release()

def simulate_readers_and_writers(num_readers, num_writers, reader_prob=0.7):
    threads = []
    student_ids = list(database.keys())

    # Crear hilos de lectores
    for i in range(num_readers):
        student_id = random.choice(student_ids)
        t = threading.Thread(target=reader, args=(i, student_id))
        threads.append(t)

    # Crear hilos de escritores
    for i in range(num_writers):
        student_id = random.choice(student_ids)
        new_grade = random.randint(50, 100)
        t = threading.Thread(target=writer, args=(i, student_id, new_grade))
        threads.append(t)

    # Mezclar orden de ejecución para simular concurrencia real
    random.shuffle(threads)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

def run_telemetry():
    print("\n" + "="*70)
    print("📊 TELEMETRÍA: Tiempo de espera de escritores vs volumen de lectores")
    print("="*70)

    test_scenarios = [
        (50, 5),   # 50 lectores, 5 escritores
        (100, 5),  # 100 lectores
        (200, 5),  # 200 lectores
        (500, 5),  # 500 lectores
    ]

    for readers, writers in test_scenarios:
        global wait_times_writers, reader_log
        wait_times_writers = []
        reader_log = defaultdict(int)

        print(f"\n🔹 Simulación con {readers} lectores y {writers} escritores")
        start_time = time.time()
        simulate_readers_and_writers(readers, writers)
        duration = time.time() - start_time

        if wait_times_writers:
            avg_wait = statistics.mean(wait_times_writers)
            max_wait = max(wait_times_writers)
            print(f"   ⏱️  Duración total: {duration:.2f}s")
            print(f"   ⏳ Tiempo de espera promedio de escritores: {avg_wait:.4f}s")
            print(f"   ⏳ Máximo tiempo de espera: {max_wait:.4f}s")
            # Mostrar distribución de lectores activos al momento de espera
            print(f"   📈 Lectores activos durante espera de escritores: {dict(reader_log)}")
        else:
            print("   ⚠️  No hubo escritores en esta simulación")

if __name__ == "__main__":
    run_telemetry()
import redis
import threading
import sys
import hashlib
import getpass
from datetime import datetime
from redis.exceptions import ResponseError

class RedisChat:
    def __init__(self, host='localhost', port=6379):
        # Подключаемся к Redis. decode_responses=True автоматически декодирует байты в строки
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.r.pubsub()
        self.channel = "global_chat"
        self.username = None
        self.user_id = None
        
        self._init_bloom_filter()

    def _init_bloom_filter(self):
        """Инициализация Фильтра Блума для проверки уникальности никнеймов"""
        try:
            # error_rate=0.01 (1% ложных срабатываний), capacity=100000
            self.r.bf().reserve("chat:usernames", 0.01, 100000)
        except ResponseError as e:
            # Если фильтр уже существует, Redis выбросит ошибку, игнорируем её
            error_msg = str(e).lower()
            if "already exists" not in error_msg and "item exists" not in error_msg:
                raise e

    def _hash_password(self, password):
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def register_and_login(self):
        """Регистрация и логин с использованием Фильтра Блума, Hashes и Bitmaps"""
        while True:
            name = input("Введите ваш никнейм: ").strip()
            if not name:
                continue

            # ИСПОЛЬЗОВАНИЕ БЛУМ ФИЛЬТРА: Быстрая проверка
            is_taken = self.r.bf().exists("chat:usernames", name)
            
            if is_taken:
                if self.r.sismember("chat:online", name):
                    print(" Этот пользователь сейчас находится в чате. Выберите другой ник.")
                    continue

                password = getpass.getpass(f"Введите пароль для {name}: ")
                hashed_input = self._hash_password(password)
                stored_hash = self.r.hget(f"user:{name}", "password")

                if hashed_input == stored_hash:
                    self.username = name
                    self.user_id = int(self.r.hget(f"user:{name}", "id"))
                    print(f" С возвращением, {self.username}! Ваш внутренний ID: {self.user_id}")
                else:
                    print(" Неверный пароль! Попробуйте снова.")
                    continue
            else:
                print(" Похоже, вы новый пользователь!")
                password = getpass.getpass("Придумайте пароль: ")
                hashed_password = self._hash_password(password)

                # Регистрация никанейма
                self.r.bf().add("chat:usernames", name)
                self.username = name
                self.user_id = self.r.incr("chat:users_count")

                self.r.hset(f"user:{name}", mapping={
                    "id": self.user_id,
                    "password": hashed_password
                })
                
                print(f" Добро пожаловать, {self.username}! Ваш внутренний ID: {self.user_id}")
            
            self.r.sadd("chat:online", self.username)

            # ИСПОЛЬЗОВАНИЕ БИТОВЫХ ОПЕРАЦИЙ (Bitmaps): Отмечаем активность
            # Ключ вида "chat:active:2023-10-25"
            today = datetime.now().strftime("%Y-%m-%d")
            bitmap_key = f"chat:active:{today}"
            
            # Устанавливаем бит под номером user_id в 1 (Пользователь онлайн сегодня)
            self.r.setbit(bitmap_key, self.user_id, 1)
            
            self._show_dau(bitmap_key)
            break

    def _show_dau(self, bitmap_key):
        """Подсчет активных пользователей за день"""
        # BITCOUNT считает количество единичек (1) в строке
        active_users = self.r.bitcount(bitmap_key)
        print(f" Активных пользователей сегодня: {active_users}")

    def show_history(self):
        """Показ истории сообщений с использованием Redis Lists"""
        print("--- Последние сообщения ---")
        history = self.r.lrange("chat:history", 0, -1)

        for msg in history:
            print(msg)
        print("---------------------------")

    def listen_messages(self):
        """Подписка на канал Pub/Sub (выполняется в отдельном потоке)"""
        self.pubsub.subscribe(self.channel)
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                msg_data = message['data']
                if not msg_data.startswith(f"[{self.username}]"):
                    sys.stdout.write(f"\r{msg_data}\n> ")
                    sys.stdout.flush()

    def send_message(self):
        """Отправка сообщений в Pub/Sub и сохранение в List"""
        listener_thread = threading.Thread(target=self.listen_messages, daemon=True)
        listener_thread.start()

        print("Вы вошли в чат! Напишите сообщение и нажмите Enter (или /quit для выхода).")
        while True:
            text = input("> ")
            if text == "/quit":
                break
            if not text.strip():
                continue

            formatted_msg = f"[{self.username}] {text}"

            self.r.rpush("chat:history", formatted_msg)
            # Ограничение истории до 200 сообщений
            self.r.ltrim("chat:history", -200, -1)
            self.r.publish(self.channel, formatted_msg)

    def start(self):
        try:
            self.register_and_login()
            self.show_history()
            self.send_message()
        except KeyboardInterrupt:
            pass
        finally:
            if self.username:
                self.r.srem("chat:online", self.username)
            print("\nВыход из чата...")

if __name__ == "__main__":
    chat = RedisChat()
    chat.start()
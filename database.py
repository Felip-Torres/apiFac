import pymysql.cursors
import hashlib
import base64
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from datetime import datetime

class database(object):
    def conecta(self):
        try:
            self.db = pymysql.connect(
                host='yamanote.proxy.rlwy.net',
                port=43070,
                user='root',
                password='QiaeXdBxTmnWdjaDBGgMrwNCqpvOSKfH',
                db='railway',
                charset='utf8mb4',
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.db.cursor()
            print("Conexión exitosa a la base de datos.")
        except pymysql.MySQLError as e:
            print(f"Error al conectar a la base de datos: {e}")
            self.db = None
            self.cursor = None


    def desconecta(self):
        self.db.close()

    def createUser(self, username: str, password: str, bio: str, image: str):
        try:
            
            # Verificar si el usuario ya existe
            existing_user = self.getUser(username)
            if existing_user:
                return {"error": "El usuario ya existe"}
    
            # Insertar el usuario en la base de datos
            query = "INSERT INTO usuarisclase VALUES (NULL, %s, %s, %s, %s)"

            self.conecta()

            self.cursor.execute(query, (username, password, bio, image))

            print("llegue por aqui")

            # Confirmar los cambios en la base de datos
            self.db.commit()  # Asegurarse de que los cambios se guarden

            # Obtener el ID del nuevo usuario
            user_id = self.cursor.lastrowid
            self.desconecta()
    
            return {"message": "Usuario registrado exitosamente", "user_id": user_id}
        except Exception as e:
            print (f"Error al crear usuario: {e.args}")
            return {"error": str(e)}


    def getUsers(self):
        self.conecta()
        sql="SELECT * from usuarisclase;"
        self.cursor.execute(sql)
        ResQuery=self.cursor.fetchall()
        self.desconecta()
        return ResQuery
    
    def getUser(self, username):
        self.conecta()
        sql="SELECT id, username, bio, image from usuarisclase where username = %s;"
        self.cursor.execute(sql, (username))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery
    
    def getUserPasswd(self, username):
        self.conecta()
        sql="SELECT password from usuarisclase where username = %s;"
        self.cursor.execute(sql, (username))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery

    def getGroups(self):
        self.conecta()
        sql="SELECT * from groups;"
        self.cursor.execute(sql)
        ResQuery=self.cursor.fetchall()
        self.desconecta()
        return ResQuery
    
    def getMessagesUsers(self, loadSize, user1_id, user2_id):
        self.conecta()
        sql = """SELECT m.*, u.username
                 FROM message m
                 JOIN usuarisclase u ON u.id = m.sender_id
                 WHERE (m.sender_id = %s AND m.receiver_id = %s)
                    OR (m.sender_id = %s AND m.receiver_id = %s)
                 ORDER BY m.date desc
                 LIMIT %s;
            """
        self.cursor.execute(sql, (user1_id, user2_id, user2_id, user1_id, loadSize))
        ResQuery = self.cursor.fetchall()
        self.desconecta()
        return ResQuery[::-1]
    
    def getImage(self, username):
        self.conecta()
        sql="select image as imageUrl from usuarisclase where username = %s;"
        self.cursor.execute(sql,(username))
        ResQuery = self.cursor.fetchone()
        self.desconecta()
        return ResQuery
    
    def getLastMessagesUsers(self, userId):
        self.conecta()
        sql= """WITH latest_messages AS (
                    SELECT 
                        LEAST(sender_id, receiver_id) AS user1,
                        GREATEST(sender_id, receiver_id) AS user2,
                        MAX(date) AS last_message_time
                    FROM message
                    WHERE sender_id = %s OR receiver_id = %s
                    GROUP BY user1, user2
                )
                SELECT 
                    m.sender_id,
                    m.receiver_id,
                    m.body AS message,
                    m.date AS time,
                    u.username AS username,
                    u.image AS imageUrl
                FROM message m
                JOIN latest_messages lm 
                    ON (LEAST(m.sender_id, m.receiver_id) = lm.user1 
                    AND GREATEST(m.sender_id, m.receiver_id) = lm.user2 
                    AND m.date = lm.last_message_time)
                JOIN usuarisclase u 
                    ON u.id = (CASE 
                                WHEN m.sender_id = %s THEN m.receiver_id 
                                ELSE m.sender_id 
                            END)
                ORDER BY m.date DESC;"""
        self.cursor.execute(sql,(userId, userId, userId))
        ResQuery = self.cursor.fetchall()
        return ResQuery
    
    def updateMessages(self, userId):
        self.conecta()
        sql= """SELECT 
                    LEAST(m.sender_id, m.receiver_id) AS user1,
                    GREATEST(m.sender_id, m.receiver_id) AS user2,
                    m.id
                FROM message m
                WHERE m.receiver_id = %s 
                    AND m.status = 'sent'
                ORDER BY user1, user2, m.date ASC;"""
        self.cursor.execute(sql,(userId))
        ResQuery = self.cursor.fetchall()
        self.desconecta()
        for message in ResQuery:
            self.checkMessage(message['id'])
            print(message['id'])
        return
    
    def readMessages(self, user1, user2):
        self.conecta()
        sql= """SELECT 
                    id
                FROM message
                WHERE receiver_id = %s AND sender_id = %s
                    AND status = 'received';"""
        self.cursor.execute(sql,(user1, user2))
        ResQuery = self.cursor.fetchall()
        self.desconecta()
        for message in ResQuery:
            self.checkMessage(message['id'])
        return
    
    def getMessagesGroups(self, loadSize, group_id):
        self.conecta()
        sql = """
                SELECT m.* FROM message m
                JOIN groups g ON g.id = m.group_id
                WHERE m.group_id = %s
                ORDER BY m.date DESC
                LIMIT %s;
        """
        self.cursor.execute(sql, (group_id, loadSize))
        ResQuery = self.cursor.fetchall()
        self.desconecta()
        return ResQuery

    def getFriends(self, userId):
        self.conecta()
        sql="SELECT id, username, password, bio, image FROM usuarisclase WHERE id != %s"
        self.cursor.execute(sql, (userId))
        ResQuery=self.cursor.fetchall()
        self.desconecta()
        return ResQuery
    
    def getUserGroup(self):
        self.conecta()
        sql="SELECT * from user_group;"
        self.cursor.execute(sql)
        ResQuery=self.cursor.fetchall()
        self.desconecta()
        return ResQuery
    
    def getUserId(self, username):
        self.conecta()
        sql='SELECT id from usuarisclase where username = %s;'
        self.cursor.execute(sql, (username))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if not ResQuery:
            raise Exception("Usuario no encontrado")
        return ResQuery['id']
    
    def getUsername(self, userId):
        self.conecta()
        sql="SELECT username FROM usuarisclase WHERE id = %s"
        self.cursor.execute(sql, (userId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery['username']
        
    def getGroupId(self, group_name):
        self.conecta()
        sql='SELECT id from groups where name = %s;'
        self.cursor.execute(sql, (group_name))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if not ResQuery:
            raise Exception("Grupo no encontrado")
        return ResQuery['id']

    def userExists(self, userId):
        self.conecta()
        sql='SELECT * from usuarisclase where id = %s;'
        self.cursor.execute(sql, (userId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if ResQuery:
            return True
        return False
    
    def deleteUser(self, userId):
        if not self.userExists(userId):
            raise Exception("Usuario no encontrado")
        self.conecta()
        sql='DELETE from usuarisclase where id = %s;'
        self.cursor.execute(sql, (userId))
        self.desconecta()
        return
    
    def groupExists(self, groupId):
        self.conecta()
        sql='SELECT * from groups where id = %s;'
        self.cursor.execute(sql, (groupId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if ResQuery:
            return True
        return False
    
    def deleteGroup(self, groupId):
        if not self.groupExists(groupId):
            raise Exception("Grupo no encontrado")
        self.conecta()
        sql='DELETE from groups where id = %s;'
        self.cursor.execute(sql, (groupId))
        self.desconecta()
        return
    
    def messageExists(self, messageId):
        self.conecta()
        sql='SELECT * from message where id = %s;'
        self.cursor.execute(sql, (messageId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if ResQuery:
            return True
        return False
    
    def deleteMessage(self, messageId):
        if not self.messageExists(messageId):
            raise Exception("Mensaje no encontrado")
        self.conecta()
        sql='DELETE from message where id = %s;'
        self.cursor.execute(sql, (messageId))
        self.desconecta()
        return
    
    def userExistsInGroup(self, userId, groupId):
        self.conecta()
        sql='SELECT * from user_group where id_user = %s and id_group = %s;'
        self.cursor.execute(sql, (userId, groupId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if ResQuery:
            return True
        return False
       
    def createGroup(self, name, description):
        self.conecta()
        creation_date = datetime.now().strftime('%Y-%m-%d')
        sql = "INSERT INTO groups (name, description, size, creation_date) VALUES (%s, %s, 0, %s);"
        self.cursor.execute(sql, (name, description, creation_date))
        group_id = self.cursor.lastrowid
        self.desconecta()
        return group_id
    
    def addUserToGroup(self, group_id, user_id, admin=False):
        self.conecta()
        join_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO user_group (id_group, id_user, join_date, admin) VALUES (%s, %s, %s, %s);"
        self.cursor.execute(sql, (group_id, user_id, join_date, int(admin)))
        sql_update_size = "UPDATE groups SET size = size + 1 WHERE id = %s;"
        self.cursor.execute(sql_update_size, (group_id,))
        self.desconecta()
        
    def updateGroupSize(self, groupId, size):
        self.conecta()
        sql = """ UPDATE groups
                  SET size = %s
                  WHERE id = %s;"""
        self.cursor.execute(sql, (size, groupId))
        self.desconecta()
        return
    
    
    def deleteUserFromGroup(self, userId, groupId):
        if not self.userExistsInGroup(userId, groupId):
            raise Exception("User not registered in group")
        self.conecta()
        sql="DELETE from user_group where id_user = %s and id_group = %s;"
        self.cursor.execute(sql, (userId, groupId))
        sql2="UPDATE groups SET size = size - 1 WHERE id = %s;"
        self.cursor.execute(sql2, (groupId))
        sql3="SELECT size FROM groups WHERE id = %s"
        self.cursor.execute(sql3, (groupId))
        size=self.cursor.fetchone()   
        if size['size'] == 0:
            self.deleteMessagesAndGroup(groupId)
        self.desconecta()
        return
    
    def deleteMessagesAndGroup(self, groupId):
        self.conecta()
        sql="DELETE FROM message WHERE group_id = %s"
        self.cursor.execute(sql, (groupId))
        sql2="DELETE FROM groups WHERE id = %s AND size = 0;"
        self.cursor.execute(sql2, (groupId))       
        self.desconecta()
        
    def getUpdatedUser(self, userId):
        self.conecta()
        sql="SELECT username, bio FROM usuarisclase WHERE id = %s"
        self.cursor.execute(sql, (userId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery
    
    def setMessageStatus(self, messageId, newStatus):
        self.conecta()
        sql = 'UPDATE message set status = %s where id = %s;'
        self.cursor.execute(sql, (newStatus, messageId))
        self.desconecta()
        return

    def checkMessage(self, messageId):
        if not self.messageExists(messageId):
            raise Exception("Mensaje no encontrado")
        self.conecta()
        sql='SELECT status from message where id = %s;'
        self.cursor.execute(sql, (messageId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        if ResQuery['status'] == 'sent':
            self.setMessageStatus(messageId, 'received')
        elif ResQuery['status'] == 'received':
            self.setMessageStatus(messageId, 'seen')
        elif ResQuery['status'] == 'seen':
            raise Exception("Message already seen")
        return
    
    def sendGroupMessage(self, message: dict):
        self.conecta()
        sql = "INSERT INTO message (date, status, body, sender_id, group_id) VALUES (%s, %s, %s, %s, %s);"
        self.cursor.execute(sql, (message['date'], message['status'], message['body'], message['sender_id'], message['group_id']))
        self.desconecta()
        return 

    def get_user_id_by_name(self, username):
        sql = "SELECT id FROM usuarisclase WHERE username = %s"
        self.cursor.execute(sql, (username,))
        result = self.cursor.fetchone()
        return result['id'] if result else None
    
    def sendUsersMessage(self, message: dict):
        self.conecta()

        sender_id = self.get_user_id_by_name(message['sender'])
        
        sql = "INSERT INTO message (date, status, body, sender_id, receiver_id) VALUES (%s, %s, %s, %s, %s);"
        self.cursor.execute(sql, (message['date'], message['status'], message['body'], sender_id, message['receiver']))
        self.desconecta()
        return

    def isUserAdmin(self, userId, groupId):
        self.conecta()
        sql="SELECT * from user_group where id_user = %s and id_group = %s;"
        self.cursor.execute(sql, (userId, groupId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()    
        return ResQuery     
    
    def updateUserAdminStatus(self, userId, groupId):
        user = self.isUserAdmin(userId, groupId)
        newStatus = 0 if user['admin'] == 1 else 1         
        self.conecta()
        sql="UPDATE user_group SET admin = %s WHERE id_user = %s AND id_group = %s;"
        self.cursor.execute(sql, (newStatus, userId, groupId))
        self.cursor.fetchone()
        self.desconecta()

    def infOfGroup(self, groupId):
        self.conecta()
        sql="SELECT * FROM user_group WHERE id_group = %s;"
        self.cursor.execute(sql, (groupId))
        ResQuery=self.cursor.fetchall()
        self.desconecta()
        return ResQuery

    def getClientUser(self, username):
        self.conecta()
        sql='select id, username, bio from usuarisclase where username = "%s";'
        self.cursor.execute(sql, (username))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery
    
    def loginCorrect(self, userId):
        self.conecta()
        sql='select password AS contraseña_encriptada from usuarisclase where id = %s;'
        self.cursor.execute(sql, (userId))
        ResQuery=self.cursor.fetchone()
        self.desconecta()
        return ResQuery['contraseña_encriptada']
    
    def updateUserProfile(self, userId, username, bio):
        self.conecta()
        sql = "UPDATE usuarisclase SET username= %s, bio= %s WHERE id= %s;"
        self.cursor.execute(sql, (username, bio, userId))
        self.desconecta()

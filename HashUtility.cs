using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace GPRestApi.Infrastructure
{
    public static class HashUtility
    {
        private static string GetMd5Hash(string input)
        {
            var md5Hash = System.Security.Cryptography.MD5.Create(); // static property not thread safe
            var data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sBuilder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));//format each one as a hexadecimal string.
            }
            return sBuilder.ToString();
        }

        /// <summary>
        /// Hashes the string and stores it in a table. This method will create the table if it doesn't exist. Requires permission to create a table.
        /// </summary>
        /// <param name="conn">Pass in the connection it is assumed the calling methods already have a connection open.</param>
        /// <param name="modelType">Type of Object we are hashing.</param>
        /// <param name="providerKey">Record Identifier unique to the Object</param>
        /// <param name="json">Json string of the object to be hashed. Do not include the timestamp or modified date as that will change even if the fields we want do not.</param>
        /// <returns>True if the hash was inserted or updated. False if we have the record and the hash matches.</returns>
        public static bool SaveHash(SqlConnection conn, string modelType, string providerKey, string json)
        {
            var hash = GetMd5Hash(json);
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }

            var command = new SqlCommand("IF OBJECT_ID('[dbo].[CBSynchronizationHash]', 'U') IS NOT NULL SELECT 'true' ELSE SELECT 'false'", conn);
            var tableExists = Convert.ToBoolean(command.ExecuteScalar());

            if (!tableExists)
            {
                using (var cmd = new SqlCommand("CREATE TABLE CBSynchronizationHash(ModelType char(30),ProviderKey char(50),CBHash char(150), [AddedAt] [datetime], [UpdatedAt] [datetime])", conn))
                {
                    cmd.ExecuteScalar();
                }
            }

            var existingHash = string.Empty;
            using (var selectHashCommand = new SqlCommand("SELECT [CBHash] FROM [dbo].[CBSynchronizationHash] WHERE [ModelType] = @modelType AND [ProviderKey] = @providerKey", conn))
            {
                selectHashCommand.Parameters.AddWithValue("@modelType", modelType);
                selectHashCommand.Parameters.AddWithValue("@providerKey", providerKey);
                var storedHashValue = selectHashCommand.ExecuteScalar();
                existingHash = storedHashValue?.ToString().Trim();
            }

            // If the hash doesn't already exist add it
            if (string.IsNullOrEmpty(existingHash))
            {
                using (var insertCommand = new SqlCommand("INSERT INTO [dbo].[CBSynchronizationHash]([ModelType],[ProviderKey],[CBHash],[AddedAt]) VALUES(@modelType,@providerKey,@cbhash,@addedAt)", conn))
                {
                    insertCommand.Parameters.AddWithValue("@modelType", modelType);
                    insertCommand.Parameters.AddWithValue("@providerKey", providerKey);
                    insertCommand.Parameters.AddWithValue("@cbhash", hash);
                    insertCommand.Parameters.AddWithValue("@addedAt", DateTime.UtcNow);
                    insertCommand.ExecuteNonQuery();
                }
                return true;
            }

            // if the Hash matches we already have this record
            if (string.Equals(hash, existingHash))
            {
                return false;
            }

            // the hash doesn't match something changed we need this updated record
            using (SqlCommand cmd = new SqlCommand("UPDATE [dbo].[CBSynchronizationHash] SET [CBHash] = @cbhash, [UpdatedAt] = @updatedAt WHERE [ModelType] = @modelType AND [ProviderKey] = @providerKey", conn))
            {
                cmd.Parameters.AddWithValue("@modelType", modelType);
                cmd.Parameters.AddWithValue("@providerKey", providerKey);
                cmd.Parameters.AddWithValue("@cbhash", hash);
                cmd.Parameters.AddWithValue("@updatedAt", DateTime.UtcNow);
                cmd.ExecuteNonQuery();
            }
            return true;
        }
    }
}
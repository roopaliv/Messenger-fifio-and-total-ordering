package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by paali on 3/4/17.
 */
import android.database.sqlite.*;
import android.content.*;
import android.database.Cursor;
import android.util.Log;
//COde taken from PA_2A
public class MessageStoreOpenHandler extends SQLiteOpenHelper{
    private static final int version = 9;
    private static final String tableName = "Messages";
    private static final String dbName = "MessageDb";
    private static final String keyColumn = "key";
    private static final String valueColumn = "value";
    private static final String messageCreateStatement = "CREATE TABLE " + tableName + " (" + keyColumn + " TEXT PRIMARY KEY, " + valueColumn + " TEXT);";

    MessageStoreOpenHandler(Context context) {
        super(context, dbName, null, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(messageCreateStatement);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + tableName);
        onCreate(db);
    }

    public Cursor getMessageByKey(String key) {
        try {
            SQLiteDatabase db = this.getReadableDatabase();
            Cursor returnedRows =  db.rawQuery( "select * from "+tableName+" where "+keyColumn+"='"+key+"'", null );
            return returnedRows;
        }
        catch (Exception e){
            Log.e("getMessageByKey", e.getMessage());
            return  null;
        }
    }

    public boolean updateMessage (String key, String value) {
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues contentValues = new ContentValues();
        contentValues.put("value", value);
        db.update(tableName, contentValues, keyColumn+" = ? ", new String[] { key } );
        return true;
    }

    public boolean insertMessage (String key, String value) {
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", key);
            contentValues.put("value", value);
            //db.insert(tableName, null, contentValues);
            db.insertWithOnConflict(tableName, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            return true;
        }
        catch(Exception e) {
            Log.e("insertMessage", e.getMessage());
            return false;
        }
    }
}

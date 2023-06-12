import java.util.List;
import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


public class Index implements Serializable{
    public Map<String,List<String>> file_recipe;
    public Map<String,byte[]> chunks;
    int total;
    int unique;
    double s1;
    double s2;

    public Index () {
        this.file_recipe = new HashMap<String, List<String>>();
        this.chunks = new HashMap<String, byte[]>();
        this.total = 0;
        this.unique = 0;
        this.s1 = 0;
        this.s2 = 0;
    }
    
    public void uploadChunk(String h, String file_path, byte [] buff){

        if(!file_recipe.containsKey(file_path)){
            file_recipe.put(file_path, new ArrayList<String>());
        }
        file_recipe.get(file_path).add(h);

        if (!chunks.containsKey(h)){
            chunks.put(h, buff);
        }

    }
    public int check_chunks(String h){
        if (chunks.containsKey(h)){
            return 1;
        }else{
            return 0;
        }
    }
    public void writeIndex (String file_name, Index index){
        try{

            File file = new File(file_name);
            if(!file.exists()){
                file.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream os = new ObjectOutputStream(fos);
            os.writeObject(index);
            os.close();
            fos.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public Index readIndex (String file_name){
        try{

            File file = new File(file_name);
            if(!file.exists()){
                System.out.println("File does not exist");
                Index index = new Index();
                this.writeIndex("MyDedup.index", index);
                System.out.println("File created");
                return index;
            }
            FileInputStream fos = new FileInputStream(file);
            ObjectInputStream os = new ObjectInputStream(fos);
            Index index = (Index) os.readObject();
            os.close();
            return index;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

}

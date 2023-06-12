import java.util.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.File;

public class Container implements Serializable{
    public ArrayList<byte []> unique_chunk_list;
    public int size;
    public String container_path;
    public int container_cnt;
    public Container(String path){
        this.unique_chunk_list = new ArrayList<byte []>();
        this.container_path = path;
        this.size = 0;
        this.container_cnt=1;

    }
    
    
  
    public void add_unique(byte[] buff, int offset, int length, String hash_val){
        if (this.size + length > 1048576){
            //System.out.println("create new container");
            writeContainer(this.container_path, "container" + this.get_container_cnt(), this.unique_chunk_list);
            this.container_cnt +=1;
            this.unique_chunk_list = new ArrayList<byte[]>();
            this.size = 0;

        }
        
            if (!unique_chunk_list.contains(buff)){
                this.unique_chunk_list.add(buff);
                this.size += length;
                //System.out.println("Container have : " + buff);
            }
              
        
    }
    public void writeContainer(String foldername, String filename, ArrayList<byte []> unique_chunk_list){
        

        try{
            File file = new File("data//" + foldername.substring(foldername.lastIndexOf("/")+1) + filename);
		


            if(!file.exists()){
                file.createNewFile();
                System.out.println("create new data file");
            }
            ArrayList<byte[]> chunkdata = new ArrayList<byte[]>();
            for (byte[] cd : unique_chunk_list){
                //System.out.println("chunkdata = " + cd);
                chunkdata.add(cd);
            }
            FileOutputStream fos1 = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos1);
            oos.writeObject(chunkdata);
            oos.flush();
            oos.close();
            fos1.flush();
            fos1.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    public int check_container_size(List<byte[]> unique_chunk_list){
        return 0;
    }
    public int get_container_cnt(){
        return this.container_cnt;
    }
    public ArrayList<byte []> get_unique_chunk_list(){
        return this.unique_chunk_list;
    }

}

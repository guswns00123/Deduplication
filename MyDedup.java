import java.io.IOException;
import java.io.ObjectInput;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;




public class MyDedup implements Serializable{
    
    static class Chunk {
        byte[] chunk_data;
        int offset = 0;
        int len = 0;
        String hash_val = "";
        public Chunk (byte[] chunk_data, int len){
            this.chunk_data = chunk_data;
            this.len = len;
        }
    }
    static int mod(int n, int q){
        return n & (q - 1);
    }
    static void decode(Chunk chunk){
        for (int i = chunk.offset ; i < chunk.offset+ chunk.len ; i++){
            System.out.print(chunk.chunk_data[i]-48);
        }
        System.out.println();
    }
    static int total_chunk_size(List<Chunk> chunks){
        int sum = 0;
        for (int i = 0 ; i < chunks.size() ; i ++){
            sum += chunks.get(i).len;
        }

        return sum;
    }
    public static void upload(int min_ch, int avg_ch, int max_ch, int d, String file_path){

	File Folder = new File("data/");
	if (!Folder.exists()){
		try{
			Folder.mkdir();
			System.out.println("data folder created");
		}catch (Exception e){
			e.getStackTrace();
		}
	}
        byte[] data = null;
        File uploadfile = new File(file_path);
        //System.out.println(uploadfile);
        try{
            data = Files.readAllBytes(uploadfile.toPath());
            double bytes = Files.size(uploadfile.toPath());
            //System.out.println("total data size :" + bytes);
        }catch (IOException e1){
            e1.printStackTrace();
        }

        int rfp = 0;
        int w = min_ch;
        List<Chunk> chunk_list = new ArrayList<Chunk>();
        
       

        Chunk cur_chunk = new Chunk(data, min_ch);

        for (int j = 0 ; j < data.length+1 ; j ++){
            if (max_ch >= data.length){
                chunk_list.add(new Chunk(data, data.length));
                break;
            }
 
            //last chunk
            if (cur_chunk.offset + max_ch >= data.length){
                
                cur_chunk.len = data.length - cur_chunk.offset -1;
		//System.out.println(cur_chunk.len);
                chunk_list.add(cur_chunk);
                break;

            }
          
            //chunk size >= max_chunk_size
            if(cur_chunk.len >= max_ch){
                cur_chunk.len = max_ch;
                chunk_list.add(cur_chunk);
                Chunk next_chunk = new Chunk(cur_chunk.chunk_data, min_ch);
                next_chunk.offset = cur_chunk.offset + max_ch;
                cur_chunk = next_chunk;
               // System.out.println("next offset = "+ cur_chunk.offset);
                //System.out.println("chunk length = " + cur_chunk.len);
                cur_chunk.len = min_ch;
            }
            
            //Calculate RFP
            if (j == 0){
                for (int i = 0 ; i < min_ch ; i ++){
                    rfp += data[i] * (int) Math.pow(d,w-1);
                    w -=1;
                }
                rfp = mod(rfp,avg_ch);
                
            }else{
                int prev =  rfp;
                //System.out.println("prev = " + prev);
                int d_product = mod((int) Math.pow(d,(min_ch-1)) * data[j], avg_ch);
                int ts_product = mod(data[j+min_ch-1], avg_ch);
                rfp = mod(d * (prev - d_product) + ts_product, avg_ch);
                //System.out.println("next rfp = " + rfp);
            }

            //System.out.println(" rfp = " + j +" " +rfp);
            //check anchor point
            if (rfp == 0){
                //System.out.println("chunk length = " + cur_chunk.len);
                
                chunk_list.add(cur_chunk);
                Chunk next_chunk = new Chunk(cur_chunk.chunk_data, min_ch);
                next_chunk.offset = cur_chunk.offset + cur_chunk.len;
                cur_chunk = next_chunk;

                //System.out.println("next offset = "+ cur_chunk.offset);
             
            }

            cur_chunk.len+=1;

        }
        Index index = new Index();
        index = index.readIndex("MyDedup.index");
        
        
        Container c1 = new Container(file_path);
        
        double s1= 0;
        double s2= 0;
        // System.out.println("Before Upload : " + index.chunks.size());
        // System.out.println("Before Upload : " + index.file_recipe.size());
        int unique = 0;
        for (int i = 0; i < chunk_list.size() ; i ++){
            //System.out.println("each chunk length = " + chunk_list.get(i).len);
            //decode(chunk_list.get(i));
            Chunk c = chunk_list.get(i);
            String cc = new String(c.chunk_data);
            byte[] buff;
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("SHA-1");
                md.update(c.chunk_data, c.offset, c.len);
                byte[] checksumBytes = md.digest();
                String checksumStr = new BigInteger(1, checksumBytes).toString();
                buff = java.util.Arrays.copyOfRange(c.chunk_data,c.offset,c.len+c.offset);
                c.hash_val = checksumStr;
                s1 += c.len;
		//System.out.println(c.len);
                c1.add_unique(buff, c.offset, c.len, c.hash_val);
                if (index.check_chunks(c.hash_val) == 0){
                    s2 += c.len;
		//System.out.println(c.len);
                    unique += 1;
                }
                index.uploadChunk(c.hash_val,file_path, buff);
            } catch (NoSuchAlgorithmException e) {
                
                e.printStackTrace();
            }

            
        }
        
        //System.out.println("file recipe chunklist : " + index.file_recipe.get(file_path));
        index.total = chunk_list.size();
        index.unique = unique;
        index.s1 = s1;
        index.s2 = s2;
        index.writeIndex("MyDedup.index", index);
        if (c1.size < 1048576 && c1.size != 0){
            c1.writeContainer(file_path, "container" + c1.get_container_cnt(), c1.get_unique_chunk_list());
        }
        
        Index result = new Index();
        result = result.readIndex("MyDedup.index");
        double s3 = 0;
        if (s2 == 0){
            s3 = 0;
        }else{
            s3 = result.s1/result.s2;
        }
        
        
        
        // System.out.println("After upload : " + result.chunks.size());
        // System.out.println("After upload : " + result.file_recipe.size());
        System.out.println("Report Output:");
        System.out.println("Total number of files that have been stored: " + result.file_recipe.size());
        System.out.println("Total number of pre-deduplicated chunks in storage: " + result.total);
        System.out.println("Total number of unique chunks in storage: " + result.unique);
        System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + result.s1);
        System.out.println("Total number of bytes of unique chunks in storage: "+ result.s2);
        System.out.println("Total number of containers in storage: " + c1.get_container_cnt());
        System.out.printf("Deduplication ratio: %.2f\n", s3);
    }

    static void download(Index index, String download_path, String local_file){
        ArrayList<byte[]> chunk_data = new ArrayList<byte[]>();
        ArrayList<byte[]> all_chunk_data = new ArrayList<byte[]>();
        
        if(!index.file_recipe.containsKey(download_path)){
            System.err.println("No file");
            System.exit(1);
        }
        try{
	    
            FileOutputStream out = new FileOutputStream(local_file);
            
            String path = "data//";

            //System.out.println(path);
            File Folder = new File(path);
            for (File file : Folder.listFiles()) {
                FileInputStream in = new FileInputStream(path + file.getName());
                
                ObjectInputStream ois = new ObjectInputStream(in);
		
                chunk_data = (ArrayList<byte[]>) ois.readObject();
                for (byte[] data : chunk_data){
                    all_chunk_data.add(data);
                    //.out.println(data);
                }
                
                ois.close();
                in.close();
            }
             //System.out.println(index.file_recipe.get(local_file));
            for (String checksumstr : index.file_recipe.get(download_path)){
		        //System.out.println(checksumstr);
                //System.out.println(index.chunks.get(checksumstr));
                out.write(index.chunks.get(checksumstr));
                // if (all_chunk_data.contains(index.chunks.get(checksumstr))){
                //     System.out.print("write?");
                    
                // }
            }
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("Complete");
        

    }



    public static void main(String[] args){
        
        if (args.length != 6 && args.length != 3){
            System.err.println("Invalid input");
            System.exit(1);
        }
        
        
        if (args[0].equals("upload")){
            System.out.println("Start Upload");
            upload(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]),Integer.parseInt(args[4]), args[5]);
            

        }else if(args[0].equals("download")){
            Index index = new Index();
            index = index.readIndex("MyDedup.index");
            
            System.out.println("Start Download");
            //System.out.println(index.file_recipe);
            String download_path = args[1];
            String local_file_name = args[2];
            download(index, download_path, local_file_name);

            
        }else{
            System.err.println("Invalid operation");
            System.exit(1);
        }
    }
}

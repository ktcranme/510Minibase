package BigT;
import chainexception.*;

public class InvalidMapSizeException extends ChainException{

   public InvalidMapSizeException()
   {
      super();
   }
   
   public InvalidMapSizeException(Exception ex, String name)
   {
      super(ex, name); 
   }

}


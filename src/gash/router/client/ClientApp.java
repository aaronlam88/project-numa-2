package gash.router.client;

public class ClientApp {

	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("usage: client <address> <port>");
		}
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		
		try {
			MessageClient mc = new MessageClient(host, port);
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			CommConnection.getInstance().release();
		}
	}
}

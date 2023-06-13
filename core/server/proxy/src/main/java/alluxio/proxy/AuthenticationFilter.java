package alluxio.proxy;


import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import sun.misc.BASE64Decoder;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;

public class AuthenticationFilter implements Filter {

    private static String username = Configuration.getString(PropertyKey.PROXY_HTTP_AUTH_USERNAME);
    private static String password = Configuration.getString(PropertyKey.PROXY_HTTP_AUTH_PASSWORD);

    @Override
    public void init(FilterConfig filterConfig) {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws java.io.IOException, ServletException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        String authorization = httpServletRequest.getHeader("Authorization");
        Boolean authResult = false;
        if (authorization != null && !authorization.equals("")) {
            String userAndPass = new String(new BASE64Decoder().decodeBuffer(authorization.split(" ")[1]));
            if (userAndPass.split(":").length == 2) {
                String user = userAndPass.split(":")[0];
                String pass = userAndPass.split(":")[1];
                if (user.equals(username) && pass.equals(password)) {
                    authResult = true;
                    chain.doFilter(request, response);
                }
            }
        }
        if (!authResult) {
            httpServletResponse.setCharacterEncoding("utf-8");
            PrintWriter out = httpServletResponse.getWriter();
            httpServletResponse.setStatus(401);
            httpServletResponse.setHeader("WWW-Authenticate", "Basic realm=\"input username and password\"");
            out.print("401 Authentication failed");
        }
    }

    @Override
    public void destroy() {

    }
}

<%
String bf = request.getParameter("bf");
if(bf != null){
    response.sendRedirect(bf);
}else{
%>
Authentication error
<%}%>
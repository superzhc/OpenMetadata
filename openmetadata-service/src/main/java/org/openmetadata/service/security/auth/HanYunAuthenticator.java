package org.openmetadata.service.security.auth;

import static org.openmetadata.service.resources.teams.UserResource.USER_PROTECTED_FIELDS;

import at.favre.lib.crypto.bcrypt.BCrypt;
import freemarker.template.TemplateException;
import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.api.security.AuthenticationConfiguration;
import org.openmetadata.schema.auth.BasicAuthMechanism;
import org.openmetadata.schema.auth.LoginRequest;
import org.openmetadata.schema.auth.RefreshToken;
import org.openmetadata.schema.entity.teams.AuthenticationMechanism;
import org.openmetadata.schema.entity.teams.User;
import org.openmetadata.service.Entity;
import org.openmetadata.service.OpenMetadataApplicationConfig;
import org.openmetadata.service.auth.JwtResponse;
import org.openmetadata.service.jdbi3.TokenRepository;
import org.openmetadata.service.jdbi3.UserRepository;
import org.openmetadata.service.util.*;

@Slf4j
public class HanYunAuthenticator implements AuthenticatorHandler {
  private UserRepository userRepository;
  private TokenRepository tokenRepository;
  private AuthenticationConfiguration authenticationConfiguration;

  private static final HanYunAuthenticator INSTANCE = new HanYunAuthenticator();

  public static HanYunAuthenticator getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(OpenMetadataApplicationConfig config) {
    this.userRepository = (UserRepository) Entity.getEntityRepository(Entity.USER);
    this.tokenRepository = Entity.getTokenRepository();
    this.authenticationConfiguration = config.getAuthenticationConfiguration();
  }

  @Override
  public RefreshToken createRefreshTokenForLogin(UUID currentUserId) {
    // just delete the existing token
    RefreshToken newRefreshToken = TokenUtil.getRefreshToken(currentUserId, UUID.randomUUID());
    // save Refresh Token in Database
    tokenRepository.insertToken(newRefreshToken);

    return newRefreshToken;
  }

  @Override
  public JwtResponse loginUser(LoginRequest loginRequest) throws IOException, TemplateException {
    // 低代码用户中心返回的授权码
    String code = loginRequest.getEmail();

    Map<String, String> tokenParams = new HashMap<>();
    tokenParams.put("client_id", authenticationConfiguration.getClientId());
    tokenParams.put("client_secret", authenticationConfiguration.getClientSecret());
    tokenParams.put("grant_type", authenticationConfiguration.getGrantType());
    tokenParams.put("code", code);
    String tokenUrl =
        HttpClientUtils.appendParams(
            authenticationConfiguration.getAuthority() + "/hanyun/sys/sso/oauth/token", tokenParams);
    String tokenResult = HttpClientUtils.doPost(tokenUrl);
    LOG.info("code解析token result: " + tokenResult);
    JsonObject tokenResultJson = JsonUtils.readJson(tokenResult).asJsonObject();

    String accessToken = tokenResultJson.getString("access_token");
    LOG.info("token result 解析accessToken : " + accessToken);

    Map<String, String> userInfoParams = new HashMap<>();
    userInfoParams.put("access_token", accessToken);
    userInfoParams.put("client_id", authenticationConfiguration.getClientId());
    String userInfoResult =
        HttpClientUtils.doGet(
            authenticationConfiguration.getAuthority() + "/hanyun/sys/sso/oauth/user-info", userInfoParams);
    LOG.info("accessToken 解析 userInfoResult : " + userInfoResult);

    JsonObject userInfoJson = JsonUtils.readJson(userInfoResult).asJsonObject();

    String userName = userInfoJson.getString("loginName");

    // 自登录
    User user;
    String email = userName + "@xgit.com";
    boolean exist = userRepository.checkEmailAlreadyExists(email);
    if (!exist) {
      // 2024年5月6日 汉云单点登陆，用户不存在的情况下自动同步该用户到元数据系统中
      User newUser =
          new User()
              .withId(UUID.randomUUID())
              .withName(userName)
              .withFullyQualifiedName(userName)
              .withEmail(email)
              .withIsBot(false)
              .withIsAdmin(false)
              .withIsEmailVerified(true)
              .withUpdatedBy(userName)
              .withUpdatedAt(System.currentTimeMillis())
          //                  .withTeams(EntityUtil.toEntityReferences(create.getTeams(), Entity.TEAM))
          //                  .withRoles(EntityUtil.toEntityReferences(create.getRoles(), Entity.ROLE))
          ;

      String genPWD = PasswordUtil.generateRandomPassword();
      LOG.info("用户：{}，密码：{}", userName, genPWD);
      String newHashedPwd = BCrypt.withDefaults().hashToString(12, genPWD.toCharArray());
      newUser.withAuthenticationMechanism(
          new AuthenticationMechanism()
              .withAuthType(AuthenticationMechanism.AuthType.BASIC)
              .withConfig(new BasicAuthMechanism().withPassword(newHashedPwd)));
      user = userRepository.createInternal(newUser);
    } else {
      user = lookUserInProvider(userName);
    }

    // 将jwt的失效设置为最大，即可认为无失效期限
    JwtResponse response = getJwtResponse(user, ChronoField.EPOCH_DAY.range().getMaximum());

    return response;
  }

  @Override
  public void checkIfLoginBlocked(String userName) {
    // ignore
  }

  @Override
  public void recordFailedLoginAttempt(String providedIdentity, User user) throws TemplateException, IOException {
    // ignore
  }

  @Override
  public void validatePassword(String providedIdentity, User storedUser, String reqPassword)
      throws TemplateException, IOException {
    // ignore
  }

  @Override
  public User lookUserInProvider(String userName) {

    return userRepository.getByName(
        null, userName, new EntityUtil.Fields(Set.of(USER_PROTECTED_FIELDS), USER_PROTECTED_FIELDS));
  }
}
